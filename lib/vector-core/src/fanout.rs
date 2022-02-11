use crate::{config::ComponentKey, event::Event};
use futures::{stream, SinkExt, Stream, StreamExt};
use futures_util::stream::FuturesUnordered;
use std::{collections::HashMap, fmt};
use tokio::sync::mpsc;
use vector_buffers::topology::channel::BufferSender;

pub enum ControlMessage {
    Add(ComponentKey, BufferSender<Event>),
    Remove(ComponentKey),
    /// Will stop accepting events until Some with given id is replaced.
    Replace(ComponentKey, Option<BufferSender<Event>>),
}

impl fmt::Debug for ControlMessage {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ControlMessage::")?;
        match self {
            Self::Add(id, _) => write!(f, "Add({:?})", id),
            Self::Remove(id) => write!(f, "Remove({:?})", id),
            Self::Replace(id, _) => write!(f, "Replace({:?})", id),
        }
    }
}

pub type ControlChannel = mpsc::UnboundedSender<ControlMessage>;

pub struct Fanout {
    sinks: Vec<(ComponentKey, Option<BufferSender<Event>>)>,
    control_channel: mpsc::UnboundedReceiver<ControlMessage>,
}

impl Fanout {
    pub fn new() -> (Self, ControlChannel) {
        let (control_tx, control_rx) = mpsc::unbounded_channel();

        let fanout = Self {
            sinks: vec![],
            control_channel: control_rx,
        };

        (fanout, control_tx)
    }

    /// Add a new sink as an output.
    ///
    /// # Panics
    ///
    /// Function will panic if a sink with the same ID is already present.
    pub fn add(&mut self, id: ComponentKey, sink: BufferSender<Event>) {
        assert!(
            !self.sinks.iter().any(|(n, _)| n == &id),
            "Duplicate output id in fanout"
        );

        self.sinks.push((id, Some(sink)));
    }

    fn remove(&mut self, id: &ComponentKey) {
        // it might not always be there because it could be in-flight, in which case it simply
        // won't get re-added after the write it aborted
        if let Some(i) = self.sinks.iter().position(|(n, _)| n == id) {
            let (_id, _removed) = self.sinks.remove(i);
        }
    }

    fn replace(&mut self, id: ComponentKey, sink: Option<BufferSender<Event>>) {
        if let Some((_, existing)) = self.sinks.iter_mut().find(|(n, _)| n == &id) {
            *existing = sink;
        } else {
            // send might have been in-flight at the time, in which case it will be cancelled
            // panic!("Tried to replace a sink that's not already present");
            self.sinks.push((id, sink));
        }
    }

    pub fn process_control_messages(&mut self) {
        while let Ok(message) = self.control_channel.try_recv() {
            self.handle_control_message(message);
        }
    }

    fn handle_control_message(&mut self, message: ControlMessage) {
        match message {
            ControlMessage::Add(id, sink) => self.add(id, sink),
            ControlMessage::Remove(id) => self.remove(&id),
            ControlMessage::Replace(id, sink) => self.replace(id, sink),
        }
    }

    async fn wait_for_replacements(&mut self) {
        while self.sinks.iter().any(|x| x.1.is_none()) {
            if let Some(msg) = self.control_channel.recv().await {
                self.handle_control_message(msg);
            } else {
                // control channel is closed? probably doesn't matter what we do
            }
        }
    }

    pub async fn send_stream(&mut self, events: impl Stream<Item = Event>) {
        let stream = events.ready_chunks(1024);
        tokio::pin!(stream);
        while let Some(events) = stream.next().await {
            self.send_all(events).await;
        }
    }

    pub async fn send_all(&mut self, events: Vec<Event>) {
        self.process_control_messages();
        self.wait_for_replacements().await;

        if self.sinks.is_empty() {
            return;
        }

        let count = self.sinks.iter().filter(|x| x.1.is_some()).count();
        // the `fanout_wait` test shows that we never actually do any sends with a `None` sink
        assert_eq!(count, self.sinks.len());

        let mut clone_army: Vec<Vec<Event>> = Vec::with_capacity(count);
        for _ in 0..(count - 1) {
            clone_army.push(events.clone());
        }
        clone_army.push(events);

        let sinks = self
            .sinks
            .drain(..)
            .map(|(id, sink)| (id, sink.expect("no missing sinks")))
            .zip(clone_army)
            .collect::<Vec<_>>();

        let mut jobs = FuturesUnordered::new();
        let mut handles = HashMap::new();

        for ((id, mut sink), events) in sinks {
            let mut blocked = None;
            let mut drain = events.into_iter();
            while let Some(event) = drain.next() {
                if let Err(event) = sink.try_send(event) {
                    blocked = Some(event);
                    break;
                }
            }
            if let Some(event) = blocked {
                let id2 = id.clone();
                let (job, handle) = futures::future::abortable(async move {
                    sink.send(event).await.expect("unit error");
                    sink.send_all(&mut stream::iter(drain).map(Ok))
                        .await
                        .expect("unit error");
                    (id2, sink)
                });
                jobs.push(job);
                handles.insert(id, handle);
            } else {
                // non-blocking sinks are first in line next time
                self.sinks.push((id, Some(sink)));
            }
        }

        if jobs.is_empty() {
            return;
        }

        loop {
            tokio::select! {
                poll_result = jobs.next() => {
                    match poll_result {
                        Some(Ok((id, sink))) => {
                            self.sinks.push((id, Some(sink)));
                        }
                        Some(Err(futures::future::Aborted)) => {
                            // sink has been removed, that's fine
                        }
                        None => {
                            break;
                        }
                    }
                }
                Some(msg) = self.control_channel.recv() => {
                    // TODO: is there any better we can do for the replace case? This essentially
                    // takes the position that the send has been initiated and only new events
                    // after that point will flow into the replacement sink. Those events could be
                    // considered "lost" in one sense, but in another, they were sent before the
                    // replacement was initiated and therefore maybe shouldn't be expected at the
                    // new sink?
                    if let ControlMessage::Remove(key) | ControlMessage::Replace(key, _) = &msg {
                        if let Some(handle) = handles.get(key) {
                            handle.abort();
                        }
                    }
                    self.handle_control_message(msg);
                }
            }
        }
    }

    pub async fn send(&mut self, event: Event) {
        self.send_all(vec![event]).await
    }
}

#[cfg(test)]
mod tests {
    use std::mem;

    use futures::{poll, StreamExt};
    use tokio::sync::mpsc::UnboundedSender;
    use tokio_test::{assert_pending, assert_ready, task::spawn};
    use vector_buffers::{
        topology::{
            builder::TopologyBuilder,
            channel::{BufferReceiver, BufferSender},
        },
        WhenFull,
    };

    use super::{ControlMessage, Fanout};
    use crate::{config::ComponentKey, event::Event, test_util::collect_ready};

    async fn build_sender_pair(capacity: usize) -> (BufferSender<Event>, BufferReceiver<Event>) {
        TopologyBuilder::standalone_memory(capacity, WhenFull::Block).await
    }

    async fn build_sender_pairs(
        capacities: &[usize],
    ) -> Vec<(BufferSender<Event>, BufferReceiver<Event>)> {
        let mut pairs = Vec::new();
        for capacity in capacities {
            pairs.push(build_sender_pair(*capacity).await);
        }
        pairs
    }

    async fn fanout_from_senders(
        capacities: &[usize],
    ) -> (
        Fanout,
        UnboundedSender<ControlMessage>,
        Vec<BufferReceiver<Event>>,
    ) {
        let (mut fanout, control) = Fanout::new();
        let pairs = build_sender_pairs(capacities).await;

        let mut receivers = Vec::new();
        for (i, (sender, receiver)) in pairs.into_iter().enumerate() {
            fanout.add(ComponentKey::from(i.to_string()), sender);
            receivers.push(receiver);
        }

        (fanout, control, receivers)
    }

    async fn add_sender_to_fanout(
        fanout: &mut Fanout,
        receivers: &mut Vec<BufferReceiver<Event>>,
        sender_id: usize,
        capacity: usize,
    ) {
        let (sender, receiver) = build_sender_pair(capacity).await;
        receivers.push(receiver);

        fanout.add(ComponentKey::from(sender_id.to_string()), sender);
    }

    fn remove_sender_from_fanout(control: &UnboundedSender<ControlMessage>, sender_id: usize) {
        control
            .send(ControlMessage::Remove(ComponentKey::from(
                sender_id.to_string(),
            )))
            .expect("sending control message should not fail");
    }

    async fn replace_sender_in_fanout(
        control: &UnboundedSender<ControlMessage>,
        receivers: &mut Vec<BufferReceiver<Event>>,
        sender_id: usize,
        capacity: usize,
    ) -> BufferReceiver<Event> {
        let (sender, receiver) = build_sender_pair(capacity).await;
        let old_receiver = mem::replace(&mut receivers[sender_id], receiver);

        control
            .send(ControlMessage::Replace(
                ComponentKey::from(sender_id.to_string()),
                Some(sender),
            ))
            .expect("sending control message should not fail");

        old_receiver
    }

    async fn start_sender_replace(
        control: &UnboundedSender<ControlMessage>,
        receivers: &mut Vec<BufferReceiver<Event>>,
        sender_id: usize,
        capacity: usize,
    ) -> (BufferReceiver<Event>, BufferSender<Event>) {
        let (sender, receiver) = build_sender_pair(capacity).await;
        let old_receiver = mem::replace(&mut receivers[sender_id], receiver);

        control
            .send(ControlMessage::Replace(
                ComponentKey::from(sender_id.to_string()),
                None,
            ))
            .expect("sending control message should not fail");

        (old_receiver, sender)
    }

    fn finish_sender_replace(
        control: &UnboundedSender<ControlMessage>,
        sender_id: usize,
        sender: BufferSender<Event>,
    ) {
        control
            .send(ControlMessage::Replace(
                ComponentKey::from(sender_id.to_string()),
                Some(sender),
            ))
            .expect("sending control message should not fail");
    }

    #[tokio::test]
    async fn fanout_writes_to_all() {
        let (mut fanout, _, receivers) = fanout_from_senders(&[2, 2]).await;
        let events = make_events(2);

        let clones = events.clone();
        fanout.send_all(clones).await;

        for receiver in receivers {
            assert_eq!(collect_ready(receiver), events);
        }
    }

    #[tokio::test]
    async fn fanout_notready() {
        let (mut fanout, _, mut receivers) = fanout_from_senders(&[2, 1, 2]).await;
        let events = make_events(2);

        // First send should immediately complete because all senders have capacity:
        let mut first_send = spawn(async { fanout.send(events[0].clone()).await });
        assert_ready!(first_send.poll());
        drop(first_send);

        // Second send should return pending because sender B is now full:
        let mut second_send = spawn(async { fanout.send(events[1].clone()).await });
        assert_pending!(second_send.poll());

        // Now read an item from each receiver to free up capacity for the second sender:
        for receiver in &mut receivers {
            assert_eq!(Some(events[0].clone()), receiver.next().await);
        }

        // Now our second send should actually be able to complete:
        assert_ready!(second_send.poll());
        drop(second_send);

        // And make sure the second item comes through:
        for receiver in &mut receivers {
            assert_eq!(Some(events[1].clone()), receiver.next().await);
        }
    }

    #[tokio::test]
    async fn fanout_grow() {
        let (mut fanout, _, mut receivers) = fanout_from_senders(&[4, 4]).await;
        let events = make_events(3);

        // Send in the first two events to our initial two senders:
        fanout.send(events[0].clone()).await;
        fanout.send(events[1].clone()).await;

        // Now add a third sender:
        add_sender_to_fanout(&mut fanout, &mut receivers, 2, 4).await;

        // Send in the last event which all three senders will now get:
        fanout.send(events[2].clone()).await;

        // Make sure the first two senders got all three events, but the third sender only got the
        // last event:
        let expected_events = [&events, &events, &events[2..]];
        for (i, receiver) in receivers.iter_mut().enumerate() {
            assert_eq!(collect_ready(receiver), expected_events[i]);
        }
    }

    #[tokio::test]
    async fn fanout_shrink() {
        let (mut fanout, control, mut receivers) = fanout_from_senders(&[4, 4]).await;
        let events = make_events(3);

        // Send in the first two events to our initial two senders:
        fanout.send(events[0].clone()).await;
        fanout.send(events[1].clone()).await;

        // Now remove the second sender:
        remove_sender_from_fanout(&control, 1);

        // Send in the last event which only the first sender will get:
        fanout.send(events[2].clone()).await;

        // Make sure the first sender got all three events, but the second sender only got the first two:
        let expected_events = [&events, &events[..2]];
        for (i, receiver) in receivers.iter_mut().enumerate() {
            assert_eq!(collect_ready(receiver), expected_events[i]);
        }
    }

    #[tokio::test]
    async fn fanout_shrink_when_notready() {
        // This test exercises that when we're waiting for all sinks to become ready for a send
        // before actually doing it, we can still correctly remove a sender that was already ready, or
        // a sender which itself was the cause of not yet being ready, or a sender which has not yet
        // been polled for readiness.
        for sender_id in [0, 1, 2] {
            let (mut fanout, control, mut receivers) = fanout_from_senders(&[2, 1, 2]).await;
            let events = make_events(2);

            // First send should immediately complete because all senders have capacity:
            let mut first_send = spawn(async { fanout.send(events[0].clone()).await });
            assert_ready!(first_send.poll());
            drop(first_send);

            // Second send should return pending because sender B is now full:
            let mut second_send = spawn(async { fanout.send(events[1].clone()).await });
            assert_pending!(second_send.poll());

            // Now read an item from each receiver to free up capacity:
            for receiver in &mut receivers {
                assert_eq!(Some(events[0].clone()), receiver.next().await);
            }

            // Drop the given sender before polling again:
            remove_sender_from_fanout(&control, sender_id);

            // Now our second send should actually be able to complete.  We'll assert that whichever
            // sender we removed does not get the next event:
            assert_ready!(second_send.poll());
            drop(second_send);

            for (i, receiver) in receivers.iter_mut().enumerate() {
                if i != sender_id {
                    assert!(
                        receiver.next().await.is_some(),
                        "receiver {} is not sender_id {} and should have the event",
                        i,
                        sender_id
                    );
                }
            }

            // let mut expected_next = [
            // Some(events[1].clone()),
            // Some(events[1].clone()),
            // Some(events[1].clone()),
            // ];
            // expected_next[sender_id] = None;

            // for (i, receiver) in receivers.iter_mut().enumerate() {
            // assert_eq!(expected_next[i], receiver.next().await);
            // }
        }
    }

    #[tokio::test]
    async fn fanout_no_sinks() {
        let (mut fanout, _) = Fanout::new();
        let events = make_events(2);

        fanout.send(events[0].clone()).await;
        fanout.send(events[1].clone()).await;
    }

    #[tokio::test]
    async fn fanout_replace() {
        let (mut fanout, control, mut receivers) = fanout_from_senders(&[4, 4, 4]).await;
        let events = make_events(3);

        // First two sends should immediately complete because all senders have capacity:
        fanout.send(events[0].clone()).await;
        fanout.send(events[1].clone()).await;

        // Replace the first sender with a brand new one before polling again:
        let old_first_receiver = replace_sender_in_fanout(&control, &mut receivers, 0, 4).await;

        // And do the third send which should also complete since all senders still have capacity:
        fanout.send(events[2].clone()).await;

        // Now make sure that the new "first" sender only got the third event, but that the second and
        // third sender got all three events:
        let expected_events = [&events[2..], &events, &events];
        for (i, receiver) in receivers.iter_mut().enumerate() {
            assert_eq!(collect_ready(receiver), expected_events[i]);
        }

        // And make sure our original "first" sender got the first two events:
        assert_eq!(collect_ready(old_first_receiver), &events[..2]);
    }

    #[tokio::test]
    async fn fanout_wait() {
        let (mut fanout, control, mut receivers) = fanout_from_senders(&[4, 4]).await;
        let events = make_events(3);

        // First two sends should immediately complete because all senders have capacity:
        let send1 = Box::pin(fanout.send(events[0].clone()));
        assert_ready!(poll!(send1));
        let send2 = Box::pin(fanout.send(events[1].clone()));
        assert_ready!(poll!(send2));

        // Now do an empty replace on the second sender, which we'll test to make sure that `Fanout`
        // doesn't let any writes through until we replace it properly.  We get back the receiver
        // we've replaced, but also the sender that we want to eventually install:
        let (old_first_receiver, new_first_sender) =
            start_sender_replace(&control, &mut receivers, 0, 4).await;

        // Third send should return pending because now we have an in-flight replacement:
        let mut third_send = spawn(async { fanout.send(events[2].clone()).await });
        assert_pending!(third_send.poll());

        // Finish our sender replacement, which should wake up the third send and allow it to
        // actually complete:
        finish_sender_replace(&control, 0, new_first_sender);
        assert!(third_send.is_woken());
        assert_ready!(third_send.poll());

        // Make sure the original first sender got the first two events, the new first sender got
        // the last event, and the second sender got all three:
        let expected_events = [&events[2..], &events];
        for (i, receiver) in receivers.iter_mut().enumerate() {
            assert_eq!(collect_ready(receiver), expected_events[i]);
        }

        assert_eq!(collect_ready(old_first_receiver), &events[..2]);
    }

    fn make_events(count: usize) -> Vec<Event> {
        (0..count)
            .map(|i| Event::from(format!("line {}", i)))
            .collect()
    }
}
