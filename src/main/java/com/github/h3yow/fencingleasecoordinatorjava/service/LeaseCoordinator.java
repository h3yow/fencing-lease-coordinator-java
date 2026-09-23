package com.github.h3yow.fencingleasecoordinatorjava.service;

import com.github.h3yow.fencingleasecoordinatorjava.dto.AcquireRequest;
import com.github.h3yow.fencingleasecoordinatorjava.dto.LeaseResponse;
import com.github.h3yow.fencingleasecoordinatorjava.dto.ReleaseRequest;
import com.github.h3yow.fencingleasecoordinatorjava.dto.RenewRequest;
import com.github.h3yow.fencingleasecoordinatorjava.dto.WriteRequest;

public interface LeaseCoordinator {

    LeaseResponse acquire(AcquireRequest request);

    LeaseResponse renew(RenewRequest request);

    LeaseResponse release(ReleaseRequest request);

    LeaseResponse write(WriteRequest request);

    LeaseResponse inspect(String resourceId);

    int reapExpired();
}
