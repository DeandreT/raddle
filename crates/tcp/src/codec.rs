use raddle::wire::{Envelope, EnvelopeLimits};

use crate::Error;

#[derive(Clone, Default)]
pub(crate) struct Codec {
    #[cfg(feature = "serialization")]
    pub(crate) selected: Option<std::sync::Arc<raddle_serialization::EndpointCodecs>>,
}

impl Codec {
    pub(crate) fn selected(&self) -> bool {
        #[cfg(feature = "serialization")]
        {
            self.selected.is_some()
        }
        #[cfg(not(feature = "serialization"))]
        {
            false
        }
    }

    pub(crate) fn encode(
        &self,
        envelope: &Envelope,
        limits: EnvelopeLimits,
        selected: bool,
    ) -> Result<Vec<u8>, Error> {
        envelope.validate(limits).map_err(|_| Error::Rejected)?;
        #[cfg(feature = "serialization")]
        if !selected && let Some(profile) = &self.selected {
            profile
                .validate_plaintext(envelope)
                .map_err(|_| Error::Rejected)?;
        }
        let bytes = if selected {
            #[cfg(feature = "serialization")]
            {
                self.selected
                    .as_ref()
                    .ok_or(Error::Rejected)?
                    .encode(envelope)
                    .map_err(|_| Error::Rejected)?
            }
            #[cfg(not(feature = "serialization"))]
            {
                return Err(Error::Rejected);
            }
        } else {
            envelope.to_vec().map_err(|_| Error::Rejected)?
        };
        if bytes.len() > limits.max_bytes {
            return Err(Error::Rejected);
        }
        Ok(bytes)
    }

    pub(crate) fn decode(
        &self,
        bytes: &[u8],
        limits: EnvelopeLimits,
        selected: bool,
    ) -> Result<Envelope, Error> {
        if bytes.len() > limits.max_bytes {
            return Err(Error::Rejected);
        }
        let envelope = if selected {
            #[cfg(feature = "serialization")]
            {
                self.selected
                    .as_ref()
                    .ok_or(Error::Rejected)?
                    .decode(bytes)
                    .map_err(|_| Error::Rejected)?
            }
            #[cfg(not(feature = "serialization"))]
            {
                return Err(Error::Rejected);
            }
        } else {
            Envelope::from_slice(bytes, limits).map_err(|_| Error::Rejected)?
        };
        envelope.validate(limits).map_err(|_| Error::Rejected)?;
        #[cfg(feature = "serialization")]
        if !selected && let Some(profile) = &self.selected {
            profile
                .validate_plaintext(&envelope)
                .map_err(|_| Error::Rejected)?;
        }
        Ok(envelope)
    }
}
