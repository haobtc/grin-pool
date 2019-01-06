use std::fmt;
use std::marker::PhantomData;

use serde::de::{Deserialize, Deserializer, SeqAccess, Visitor};
use serde::ser::{Error, Serialize, SerializeTuple, Serializer};

pub trait LargeArray<'de>: Sized {
    fn serialize<S: Serializer>(&self, ser: S) -> Result<S::Ok, S::Error>;
    fn deserialize<D: Deserializer<'de>>(de: D) -> Result<Self, D::Error>;
}

macro_rules! makeup_array {
    ($($len: expr,)+) => {
        $(
            impl<'de, T> LargeArray<'de> for [T; $len]
                where T: Default + Copy + Serialize + Deserialize<'de>
                {
                    fn serialize<S: Serializer>(&self, ser: S) -> Result<S::Ok, S::Error>
                    {
                        let mut seq = ser.serialize_tuple(self.len())?;
                        for elem in &self[..] {
                            seq.serialize_element(elem)?;
                        }
                        seq.end()
                    }

                    fn deserialize<D: Deserializer<'de>>(de: D) -> Result<[T; $len], D::Error>
                    {
                        struct ArrayVisitor<T> {
                            e: PhantomData<T>,
                        };

                        impl<'de, T> Visitor<'de> for ArrayVisitor<T>
                            where T: Default + Copy + Deserialize<'de> + Serialize
                        {

                            type Value = [T; $len];

                            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result
                            {
                                formatter.write_str(concat!("An Array of length ", $len))
                            }

                            fn visit_seq<S: SeqAccess<'de>>(self, mut seq: S) -> Result<Self::Value, S::Error>
                            {
                                let mut array = [T::default(); $len];
                                for i in 0..$len {
                                    array[i] = seq.next_element()?.unwrap();
                                }
                                Ok(array)
                            }
                        }

                        let visitor = ArrayVisitor{e: PhantomData};
                        de.deserialize_tuple($len, visitor)
                    }
                }
        )+
    }
}

makeup_array! {
    40, 42, 43, 44, 45, 46, 47, 48, 49, 50,
    56, 64, 72, 96, 100, 128, 160, 192, 200, 224, 256, 384, 512,
    768, 1024, 2048, 4096, 8192, 16384, 32768, 65536,
}
