use sender;
use deque;

#[derive(Debug)]
pub enum Placement<T> {
    Memory(T),
    Disk(usize),
}

pub type Queue<T> = deque::Queue<Placement<T>, sender::SenderSync>;
