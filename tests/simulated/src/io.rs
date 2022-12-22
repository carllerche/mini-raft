use crate::*;

/// Wrapper around a turmoil I/O adding happens-before tracking using vector
/// clocks.
#[derive(Clone)]
pub(crate) struct Io {
    turmoil: turmoil::Io<message::Envelope>,
}

impl Io {
    pub(crate) fn new(turmoil: turmoil::Io<message::Envelope>) -> Io {
        Io {
            turmoil,
        }
    }

    pub(crate) fn addr(&self) -> SocketAddr {
        self.turmoil.local_addr()
    }

    pub(crate) fn send(&self, host: SocketAddr, elapsed: u64, message: Message) {
        if let Some(log) = &self.log {
            log.send(
                host,
                self.addr(),
                self.vv.borrow().get(self.addr()),
                elapsed,
                &message,
            );
        }

        self.turmoil.send(
            host,
            message::Envelope {
                vv: self.vv.borrow().clone(),
                sender: self.turmoil.addr,
                message,
            },
        );
    }

    pub(crate) async fn recv(&self, elapsed: u64) -> (Message, SocketAddr) {
        let (
            message::Envelope {
                vv,
                message,
                sender,
            },
            addr,
        ) = self.turmoil.recv().await;
        self.join_vv(&vv);

        if let Some(log) = &self.log {
            log.recv(
                self.addr(),
                self.vv.borrow().get(self.addr()),
                sender,
                vv.get(sender),
                elapsed,
                &message,
            );
        }

        (message, addr)
    }

    pub(crate) async fn recv_from(&self, host: SocketAddr) -> Message {
        let message::Envelope { vv, message, .. } = self.turmoil.recv_from(host).await;
        self.join_vv(&vv);
        message
    }

    fn join_vv(&self, vv: &VersionVec) {
        let mut our_vv = self.vv.borrow_mut();
        our_vv.join(vv);
        our_vv.inc(self.turmoil.addr);
    }
}
