use crate::{Error, ErrorKind, Result};
use serde::{Deserialize, Serialize};

macro_rules! normalized_value {
    ($name:ident, $label:literal) => {
        #[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
        #[serde(try_from = "f32", into = "f32")]
        pub struct $name(f32);

        impl $name {
            pub const MIN: f32 = 0.0;
            pub const MAX: f32 = 1.0;

            /// Creates a validated normalized value.
            ///
            /// # Errors
            ///
            /// Returns [`ErrorKind::InvalidValue`] when `value` is non-finite
            /// or outside the inclusive `0.0..=1.0` range.
            pub fn new(value: f32) -> Result<Self> {
                if value.is_finite() && (Self::MIN..=Self::MAX).contains(&value) {
                    Ok(Self(value))
                } else {
                    Err(Error::new(
                        ErrorKind::InvalidValue,
                        concat!($label, " must be finite and between 0.0 and 1.0"),
                    ))
                }
            }

            #[must_use]
            pub const fn get(self) -> f32 {
                self.0
            }
        }

        impl TryFrom<f32> for $name {
            type Error = Error;

            fn try_from(value: f32) -> Result<Self> {
                Self::new(value)
            }
        }

        impl From<$name> for f32 {
            fn from(value: $name) -> Self {
                value.get()
            }
        }
    };
}

normalized_value!(Volume, "volume");
normalized_value!(Gain, "gain");
normalized_value!(MixBalance, "mix balance");

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum MuteState {
    Unmuted,
    Muted,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalized_values_reject_invalid_numbers() {
        for value in [-1.0, 1.1, f32::NAN, f32::INFINITY] {
            assert!(Volume::new(value).is_err());
            assert!(Gain::new(value).is_err());
            assert!(MixBalance::new(value).is_err());
        }
    }

    #[test]
    fn normalized_values_accept_endpoints() {
        assert!(Volume::new(0.0).expect("minimum").get().abs() < f32::EPSILON);
        assert!((Volume::new(1.0).expect("maximum").get() - 1.0).abs() < f32::EPSILON);
    }
}
