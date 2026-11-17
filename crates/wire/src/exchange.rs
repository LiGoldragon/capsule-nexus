//! Both Capsule Nexus sockets speak `signal`'s exchange layer.
//!
//! The querying side greets a connection once with the digest of its call
//! contract's authored source, then opens any number of exchanges on it. The
//! query of every exchange is the Call with its Caller: `public_call::Input`
//! on the public socket, `meta_call::Input` on the meta socket. The Nexus
//! answers each exchange on its identifier: a one-answer input with its one
//! `Delivery::Answer`, a Subscribe with an Answer for the acknowledgement and
//! one for each change, until it sends `Delivery::End` or the querying side
//! abandons the exchange or closes the connection.
//!
//! Framing is `signal`'s four-byte big-endian length prefix; this module adds
//! only the two ways a blocking Unix stream waits for a frame.

use std::io::Read;
use std::marker::PhantomData;
use std::os::unix::net::UnixStream;
use std::time::{Duration, Instant};

use signal::{
    Capacious, Contracted, Delivery, Dispatch, ExchangeFault, ExchangeId, ExchangeLedger,
    ExchangeMinting, FrameBody, FrameCapacity, FrameError, FrameWriting, Greeted, HandshakeReceipt,
    Opening, Restorable, Signal, Signalizable, FRAME_PREFIX_BYTES,
};

use crate::deadline::DeadlineStream;

/// The authored source of the public call contract, whose digest greets the
/// public socket.
pub const PUBLIC_CALL_ETHOS: &str = include_str!("../ethos/public-call.ethos");

/// The authored source of the meta call contract, whose digest greets the
/// meta socket.
pub const META_CALL_ETHOS: &str = include_str!("../ethos/meta-call.ethos");

impl Contracted for crate::public_call::Input {
    const CONTRACT_SOURCE: &'static str = PUBLIC_CALL_ETHOS;
}

impl Contracted for crate::meta_call::Input {
    const CONTRACT_SOURCE: &'static str = META_CALL_ETHOS;
}

/// What goes wrong for the querying side of an exchange connection.
#[derive(Debug)]
pub enum ExchangeError {
    Frame(FrameError),
    Archive(String),
    /// The Nexus refused the greeting: the two sides were built from
    /// different contract sources.
    GreetingRefused(HandshakeReceipt),
    /// The exchange layer refused what this side asked of it.
    Fault(ExchangeFault),
    /// The Nexus sent a frame this side did not ask for.
    Unexpected(&'static str),
}

impl std::fmt::Display for ExchangeError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "Capsule Nexus exchange error: {self:?}")
    }
}

impl std::error::Error for ExchangeError {}

impl From<FrameError> for ExchangeError {
    fn from(error: FrameError) -> Self {
        Self::Frame(error)
    }
}

impl From<std::io::Error> for ExchangeError {
    fn from(error: std::io::Error) -> Self {
        Self::Frame(FrameError::Io(error))
    }
}

impl From<ExchangeFault> for ExchangeError {
    fn from(fault: ExchangeFault) -> Self {
        Self::Fault(fault)
    }
}

/// Reads one whole Signal frame from a byte source that bounds its own waits.
/// `None` is the peer closing between frames.
pub fn read_frame_or_end(
    input: &mut impl Read,
    capacity: FrameCapacity,
) -> Result<Option<FrameBody>, FrameError> {
    let mut first = 0_u8;
    loop {
        match input.read(std::slice::from_mut(&mut first)) {
            Ok(0) => return Ok(None),
            Ok(_) => return rest_of_frame(first, input, capacity).map(Some),
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
            Err(error) => return Err(FrameError::Io(error)),
        }
    }
}

/// Reads one whole Signal frame from an exchange connection. The wait for its
/// first byte is unbounded, for a connection may be quiet for as long as its
/// exchanges are; once the frame has begun, the rest of it has one absolute
/// progress deadline, so a partial or drip-fed frame cannot hold the
/// connection. `None` is the peer closing between frames.
pub fn read_frame_after_idle(
    stream: &UnixStream,
    progress: Duration,
    capacity: FrameCapacity,
) -> Result<Option<FrameBody>, FrameError> {
    let mut idle = stream.try_clone()?;
    idle.set_read_timeout(None)?;
    let mut first = 0_u8;
    loop {
        match idle.read(std::slice::from_mut(&mut first)) {
            Ok(0) => return Ok(None),
            Ok(_) => break,
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
            Err(error) => return Err(FrameError::Io(error)),
        }
    }
    let mut progressing = DeadlineStream::new(idle, progress)?;
    rest_of_frame(first, &mut progressing, capacity).map(Some)
}

fn rest_of_frame(
    first: u8,
    input: &mut impl Read,
    capacity: FrameCapacity,
) -> Result<FrameBody, FrameError> {
    let mut prefix = [0_u8; FRAME_PREFIX_BYTES];
    prefix[0] = first;
    input.read_exact(&mut prefix[1..])?;
    let length = capacity.admit(u32::from_be_bytes(prefix) as usize)?;
    let mut body = vec![0_u8; length];
    input.read_exact(&mut body)?;
    Ok(FrameBody::from(body))
}

/// Restores one value of the exchange layer from a frame body.
pub fn restored<T>(body: FrameBody) -> Result<T, ExchangeError>
where
    T: rkyv::Archive,
    Signal<T>: Restorable<T>,
{
    Signal::<T>::from(Vec::from(body))
        .restore()
        .map_err(|error| ExchangeError::Archive(format!("{error:?}")))
}

/// Writes one value of the exchange layer as one frame, the whole frame
/// within `timeout`.
pub fn write_value<T: Signalizable>(
    stream: &UnixStream,
    value: &T,
    timeout: Duration,
    capacity: FrameCapacity,
) -> Result<(), ExchangeError> {
    let signal = value
        .signalize()
        .map_err(|error| ExchangeError::Archive(format!("{error:?}")))?;
    let mut output = DeadlineStream::new(stream.try_clone()?, timeout)?;
    output.write_frame(&signal, capacity)?;
    Ok(())
}

/// How long the querying side waits on the Nexus.
#[derive(Clone, Copy, Debug)]
pub enum Patience {
    /// Each frame written within the bound; each frame read waits without a
    /// bound for its first byte and then has the bound for the rest.
    Progress(Duration),
    /// Every frame written and read within one absolute deadline.
    Until(Instant),
}

impl Patience {
    fn remaining(&self) -> Result<Duration, ExchangeError> {
        match self {
            Self::Progress(bound) => Ok(*bound),
            Self::Until(deadline) => deadline
                .checked_duration_since(Instant::now())
                .filter(|remaining| !remaining.is_zero())
                .ok_or_else(|| {
                    ExchangeError::Frame(FrameError::Io(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "exchange deadline elapsed",
                    )))
                }),
        }
    }
}

/// The querying end of one connection to a Capsule Nexus socket, greeted
/// with the contract of its query `Q`, reading deliveries of its response `R`.
pub struct QueryingConnection<Q, R> {
    stream: UnixStream,
    ledger: ExchangeLedger,
    patience: Patience,
    capacity: FrameCapacity,
    contract: PhantomData<fn(Q) -> R>,
}

impl<Q, R> QueryingConnection<Q, R>
where
    Q: Contracted,
    Dispatch<Q>: Signalizable,
    R: rkyv::Archive,
    Delivery<R>: rkyv::Archive,
    Signal<Delivery<R>>: Restorable<Delivery<R>>,
{
    /// Greets a connected stream and waits for the Nexus to settle the
    /// contract.
    pub fn greeted(
        stream: UnixStream,
        patience: Patience,
        capacity: FrameCapacity,
    ) -> Result<Self, ExchangeError> {
        let mut connection = Self {
            stream,
            ledger: ExchangeLedger::default(),
            patience,
            capacity,
            contract: PhantomData,
        };
        connection.dispatch(&Dispatch::Greet(Q::greeting()))?;
        match connection.delivery()? {
            Some(Delivery::Greeted(HandshakeReceipt::Greeted(_))) => {
                connection.ledger.greet()?;
                Ok(connection)
            }
            Some(Delivery::Greeted(refused)) => Err(ExchangeError::GreetingRefused(refused)),
            Some(Delivery::End(ending)) => match ending.conclusion {
                signal::Conclusion::Faulted(fault) => Err(ExchangeError::Fault(fault)),
                signal::Conclusion::Completed => {
                    Err(ExchangeError::Unexpected("the Nexus ended the connection"))
                }
            },
            Some(Delivery::Answer(_)) => Err(ExchangeError::Unexpected(
                "the Nexus answered before settling the contract",
            )),
            None => Err(ExchangeError::Unexpected(
                "the Nexus closed before settling the contract",
            )),
        }
    }

    /// Opens an exchange carrying one query.
    pub fn open(&mut self, query: Q) -> Result<ExchangeId, ExchangeError> {
        let exchange = self.ledger.open()?;
        self.dispatch(&Dispatch::Open(Opening { exchange, query }))?;
        Ok(exchange)
    }

    /// The next frame the Nexus delivers, or `None` when it closed the
    /// connection between frames.
    pub fn delivery(&mut self) -> Result<Option<Delivery<R>>, ExchangeError> {
        let body = match self.patience {
            Patience::Progress(bound) => read_frame_after_idle(&self.stream, bound, self.capacity)?,
            Patience::Until(_) => {
                let mut input =
                    DeadlineStream::new(self.stream.try_clone()?, self.patience.remaining()?)?;
                read_frame_or_end(&mut input, self.capacity)?
            }
        };
        body.map(restored).transpose()
    }

    fn dispatch(&self, dispatch: &Dispatch<Q>) -> Result<(), ExchangeError> {
        write_value(
            &self.stream,
            dispatch,
            self.patience.remaining()?,
            self.capacity,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn a_frame_may_be_quiet_beyond_its_progress_bound() {
        let (mut writer, reader) = UnixStream::pair().expect("socket pair");
        let reading = std::thread::spawn(move || {
            read_frame_after_idle(&reader, Duration::from_millis(30), FrameCapacity::from(32))
        });
        std::thread::sleep(Duration::from_millis(75));
        writer.write_all(&3_u32.to_be_bytes()).expect("prefix");
        writer.write_all(b"new").expect("body");
        assert_eq!(
            reading.join().expect("reader thread").expect("frame"),
            Some(FrameBody::from(b"new".to_vec()))
        );
    }

    #[test]
    fn a_partial_frame_has_one_absolute_progress_deadline() {
        let (mut writer, reader) = UnixStream::pair().expect("socket pair");
        writer.write_all(&[0]).expect("first prefix byte");
        let started = Instant::now();
        let failure =
            read_frame_after_idle(&reader, Duration::from_millis(30), FrameCapacity::from(32))
                .expect_err("partial frame must time out");
        assert!(started.elapsed() < Duration::from_millis(250));
        assert!(matches!(failure, FrameError::Io(_)));
    }

    #[test]
    fn the_peer_closing_between_frames_ends_the_connection() {
        let (writer, reader) = UnixStream::pair().expect("socket pair");
        drop(writer);
        assert_eq!(
            read_frame_after_idle(&reader, Duration::from_millis(30), FrameCapacity::from(32))
                .expect("clean shutdown"),
            None
        );
    }

    #[test]
    fn an_oversized_frame_is_refused_before_its_body() {
        let mut bytes = std::io::Cursor::new(33_u32.to_be_bytes().to_vec());
        assert!(matches!(
            read_frame_or_end(&mut bytes, FrameCapacity::from(32)),
            Err(FrameError::BodyTooLarge {
                found: 33,
                capacity: 32
            })
        ));
    }

    #[test]
    fn the_two_call_contracts_greet_with_different_digests() {
        assert_ne!(
            crate::public_call::Input::contract_digest(),
            crate::meta_call::Input::contract_digest()
        );
    }
}
