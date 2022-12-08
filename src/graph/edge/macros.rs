#[macro_export]
macro_rules! edge_index {
    ($struc: ident) => {
        use std::ops::{Index, IndexMut};
        use neb::ram::types::Value;
        impl <'a> Index<u64> for $struc<'a> {
            type Output = SharedValue<'a>;
            fn index(&self, index: u64) -> &Self::Output {
                if let Some(ref cell) = self.cell {
                    &cell[index]
                } else {
                    &Value::Null
                }
            }
        }

        impl <'a, 'b> Index<&'a str> for $struc<'b> {
            type Output = SharedValue<'b>;
            fn index(&self, index: &'a str) -> &Self::Output {
                if let Some(ref cell) = self.cell {
                    &cell[index]
                } else {
                    &Value::Null
                }
            }
        }

        impl <'a, 'b> IndexMut <&'a str> for $struc<'b> {
            fn index_mut(&mut self, index: &'a str) -> &'b mut Self::Output {
                if let &mut Some(ref mut cell) = &mut self.cell {
                    &mut cell[index]
                } else {
                    panic!("this edge have no cell");
                }
            }
        }

        impl <'a> IndexMut<u64> for $struc<'a> {
            fn index_mut(&mut self, index: u64) -> &'a mut Self::Output {
                if let &mut Some(ref mut cell) = &mut self.cell {
                    &mut cell[index]
                } else {
                    panic!("this edge have no cell");
                }
            }
        }
    };
}