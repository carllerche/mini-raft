#[macro_export]
macro_rules! pos {
    ($index:literal : $term:literal) => {
        mini_raft::Pos {
            index: Index($index),
            term: Term($term),
        }
    };
}

#[macro_export]
macro_rules! val {
    ( $data:literal ) => {
        mini_raft::message::Value::data(&$data[..])
    };
    ( $f:ident $( ( $($arg:expr),*  ) )? ) => {
        mini_raft::message::Value::$f( $( $( $arg ),* )? )
    };
    ( $data:literal ) => {
        mini_raft::message::Value::data(&$data[..])
    };
    ( $value:expr ) => {
        $value
    };
}

#[macro_export]
macro_rules! entry {
    ( $index:literal : $term:literal ) => {
        entry!($index : $term, data( stringify!("data" -> $index : $term).as_bytes() ))
    };
    ( $index:literal: $term:literal, $( $t:tt )* ) => {
        mini_raft::message::Entry {
            pos: Pos {
                index: Index($index),
                term: Term($term),
            },
            value:  val!( $( $t )* ),
        }
    };
}

#[macro_export]
macro_rules! assert_observer {
    ($i:expr) => {{
        let info = $i.info();
        assert!(
            info.stage.is_observer(),
            "expected Observer but was {:?}",
            info.stage
        );
    }};
}

#[macro_export]
macro_rules! assert_follower {
    ($i:expr) => {{
        let info = $i.info();
        assert!(
            info.stage.is_follower(),
            "expected Follower but was {:?}",
            info.stage
        );
    }};
}

#[macro_export]
macro_rules! assert_candidate {
    ($i:expr) => {{
        let info = $i.info();
        assert!(
            info.stage.is_candidate(),
            "expected Candidate but was {:?}",
            info.stage
        );
    }};
}

#[macro_export]
macro_rules! assert_leader {
    ($i:expr) => {{
        let info = $i.info();
        assert!(
            info.stage.is_leader(),
            "expected Leader but was {:?}",
            info.stage,
        );
    }};
}

#[macro_export]
macro_rules! assert_term {
    ($i:expr, $term:expr) => {{
        let expect = $term;
        let actual = $i.info().term;
        assert_eq!(
            actual, expect,
            "expected term {:?}, but was {:?}",
            expect, actual
        );
    }};
}

#[macro_export]
macro_rules! assert_committed {
    ($i:expr, $index:expr) => {{
        let expect = $index;
        let actual = $i.info().committed;

        assert_eq!(
            actual.expect("expected a committed index, but was None"),
            expect
        );
    }};
}

#[macro_export]
macro_rules! assert_none_committed {
    ($i:expr) => {{
        assert!($i.info().committed.is_none());
    }};
}

#[macro_export]
macro_rules! assert_tick_at {
    ($i:expr, $dur:expr) => {{
        let expect = std::time::Duration::from_millis($dur);
        let actual = $i.tick_at().unwrap();
        assert_eq!(
            actual, expect,
            "expected next tick at {:?}, but was {:?}",
            expect, actual
        );
    }};
}

#[macro_export]
macro_rules! assert_peers {
    ($i:ident, [ $( $peer:expr ),* ]) => {{
        let mut actual: Vec<_> = $i.peers()
            .iter()
            .map(|peer| peer.id)
            .collect();
        actual.sort();

        let mut expect = vec![ $( $peer ),* ];
        expect.sort();

        assert_eq!(actual, expect);
    }}
}
