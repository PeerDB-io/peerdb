package io.peerdb.flow.jvm.iceberg.lock;


import com.google.common.util.concurrent.Striped;
import jakarta.enterprise.context.ApplicationScoped;

import java.util.concurrent.locks.Lock;

@ApplicationScoped
public class LockManager {
    int stripeCount = 10_000;

    Striped<Lock> locker = Striped.lock(stripeCount);


    // This is just abstracted out to enable changing the lock implementation in the future
    public Lock newLock(Object key) {
        return locker.get(key);
    }

}
