use sender;
use deque;

#[derive(Debug)]
pub enum Placement<T> {
    Memory(T),
    Disk(usize),
}

impl<T> Placement<T> {
    pub fn extract(self) -> Option<T> {
        match self {
            Placement::Memory(elem) => Some(elem),
            Placement::Disk(_) => None,
        }
    }
}

pub type Queue<T> = deque::Queue<Placement<T>, sender::SenderSync>;
