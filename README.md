# python_persisten_objects
A collection of python object that exists on the disk instead of existing in memory.

Tested with pyton 3, but must work also with python 2 

## BlobStore

This class take a file object and use it to store objects. These objects must be picle-able.

The function `add` return the number of bytes from the start of the file where the added object is. There a small header at the start of the object that tell us the lenght , the additional space used by the `slot` we are in, and if this slot is deleted or not.

When we delete on object we provide back it's address and we mark it's header with the deleted boolean. This add the slot to the `_holes` set. This set is used in the next add opearation to check if we have some hole where our new data can fit, and we reuse it, writing also the number of byte not used in the slot. For example the previous object was 100 bytes, the new one is 50 bytes, so we need to remember that this `_slot` is 50 bytes bigger than the size of the sotred object. This is needed when scanning over the file.

The `get` of an object is done by the pointer to it's start/header.

## 
