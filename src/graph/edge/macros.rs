#[macro_export]
macro_rules! edge_index {
    ($struc: ident) => {
        use std::ops::{Index, IndexMut};
        use neb::ram::types::{Value, NULL_VALUE};
        impl Index<u64> for $struc {
            type Output = Value;
            fn index(&self, index: u64) -> &Self::Output {
                if let Some(ref cell) = self.cell {
                    &cell[index]
                } else {
                    &NULL_VALUE
                }
            }
        }

        impl <'a> Index<&'a str> for $struc {
            type Output = Value;
            fn index(&self, index: &'a str) -> &Self::Output {
                if let Some(ref cell) = self.cell {
                    &cell[index]
                } else {
                    &NULL_VALUE
                }
            }
        }

        impl <'a> IndexMut <&'a str> for $struc {
            fn index_mut<'b>(&'b mut self, index: &'a str) -> &'b mut Self::Output {
                if let &mut Some(ref mut cell) = &mut self.cell {
                    &mut cell[index]
                } else {
                    panic!("this edge have no cell");
                }
            }
        }

        impl IndexMut<u64> for $struc {
            fn index_mut<'a>(&'a mut self, index: u64) -> &'a mut Self::Output {
                if let &mut Some(ref mut cell) = &mut self.cell {
                    &mut cell[index]
                } else {
                    panic!("this edge have no cell");
                }
            }
        }
    };
}