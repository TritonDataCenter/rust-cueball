// Copyright 2019 Joyent, Inc.

use cueball::resolver::{
    BackendMsg,
};

///
/// Given a list of BackendMsg and a BackendMsg to find, returns the index of
/// the desired item in the list, or None if the item is not in the list. We
/// return the index, rather than the item itself, to allow callers of the
/// function to more easily manipulate the list afterward.
///
pub fn find_msg_match(list: &[BackendMsg], to_find: &BackendMsg)
    -> Option<usize> {
    for (index, item) in list.iter().enumerate() {
        if item == to_find {
            return Some(index);
        }
    }
    None
}
