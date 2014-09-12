
#include "include/types.h"
#include "Messenger.h"

#include "SimpleMessenger.h"
#include "AsyncMessenger.h"

Messenger *Messenger::create(CephContext *cct,
			     entity_name_t name,
			     string lname,
			     uint64_t nonce)
{
  if (cct->_conf->ms_use_event) {
    return new AsyncMessenger(cct, name, lname, nonce);
  } else {
    return new SimpleMessenger(cct, name, lname, nonce);
  }
}
