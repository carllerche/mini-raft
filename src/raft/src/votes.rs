use crate::*;

use indexmap::IndexMap;

/// Tracks votes received from peer nodes.
pub(crate) struct Votes<T: Driver> {
    /// When the group is **not** in joint-consensus mode, votes are tracked in
    /// this set. When the group **is** in joint-consensus mode, peers in the
    /// "incoming" group are tracked here.
    primary: IndexMap<T::Id, bool>,

    /// When the group is **not** in joint-consensus mode, this map is not used.
    /// When the group **is** in joint-consensus mode, peers in the **outgoing**
    /// group are tracked here.
    secondary: IndexMap<T::Id, bool>,
}

#[derive(Debug)]
pub(crate) enum Tally {
    Win,
    Lose,
    Pending,
}

impl<T: Driver> Votes<T> {
    pub(crate) fn new() -> Votes<T> {
        Votes {
            primary: IndexMap::new(),
            secondary: IndexMap::new(),
        }
    }

    pub(crate) fn clear(&mut self) {
        self.primary.clear();
        self.secondary.clear();
    }

    pub(crate) fn record(&mut self, group: &Group<T>, id: &T::Id, granted: bool) {
        use peer::Role;

        if group.is_joint_consensus() {
            let peer = match group.peers.get(id) {
                Some(peer) => peer,
                // The message is from an unknown peer (from the perspective of
                // the current node);
                _ => return,
            };

            match peer.role {
                Role::Voter => {
                    // Votes count in both the incoming **and** outgoing group
                    self.primary.entry(id.clone()).or_insert(granted);
                    self.secondary.entry(id.clone()).or_insert(granted);
                }
                Role::VoterIncoming { .. } => {
                    self.primary.entry(id.clone()).or_insert(granted);
                }
                Role::VoterOutgoing { .. } => {
                    self.secondary.entry(id.clone()).or_insert(granted);
                }
                _ => return,
            }
        } else {
            self.primary.entry(id.clone()).or_insert(granted);
        }
    }

    pub(crate) fn tally(&self, group: &Group<T>) -> Tally {
        if group.is_joint_consensus() {
            let incoming = Tally::from_votes::<T, _>(&self.primary, group.incoming_voter_ids());
            let outgoing = Tally::from_votes::<T, _>(&&self.secondary, group.outgoing_voter_ids());

            incoming.join(outgoing)
        } else {
            Tally::from_votes::<T, _>(&self.primary, group.voter_ids())
        }
    }
}

impl Tally {
    fn from_votes<'a, T, I>(votes: &IndexMap<T::Id, bool>, voters: I) -> Tally
    where
        T: Driver,
        I: Iterator<Item = &'a T::Id>,
    {
        let mut yes = 0;
        let mut no = 0;
        let mut pending = 0;

        for voter_id in voters {
            match votes.get(voter_id) {
                Some(true) => yes += 1,
                Some(false) => no += 1,
                None => pending += 1,
            }
        }

        let threshold = ((yes + no + pending) / 2) + 1;

        if yes >= threshold {
            Tally::Win
        } else if yes + pending < threshold {
            Tally::Lose
        } else {
            assert!(
                yes + pending >= threshold,
                "yes={yes}; no={no}; pending={pending}; threshold={threshold}"
            );
            Tally::Pending
        }
    }

    fn join(self, other: Tally) -> Tally {
        use Tally::*;

        match (self, other) {
            // If either tallies are `Lose`, then candidate lost the election
            (Lose, _) | (_, Lose) => Lose,

            // If either is `Pending` with `Pending` or `Win`, then the tally is still pending.
            (Pending, _) | (_, Pending) => Pending,

            // If both tallies are `Win` then the join is also `Win`.
            (Win, Win) => Win,
        }
    }
}
