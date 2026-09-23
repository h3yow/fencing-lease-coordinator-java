package com.github.h3yow.fencingleasecoordinatorjava.tests;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.h3yow.fencingleasecoordinatorjava.Application;
import com.github.h3yow.fencingleasecoordinatorjava.config.LeaseProperties;
import com.github.h3yow.fencingleasecoordinatorjava.idem.FileIdempotencyStore;
import com.github.h3yow.fencingleasecoordinatorjava.service.LeaseCoordinator;
import com.github.h3yow.fencingleasecoordinatorjava.service.SystemClock;
import com.github.h3yow.fencingleasecoordinatorjava.storage.FileLeaseStore;
import com.github.h3yow.fencingleasecoordinatorjava.storage.WriteLedger;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.web.servlet.MockMvc;

/**
 * 集成测试基类：每个用例使用独立的临时数据目录（重建存储组件并反射注入协调器），
 * 并通过 {@link ControllableClock} 手动推进时间。
 */
@SpringBootTest(classes = Application.class)
@AutoConfigureMockMvc
@Import(TestConfig.class)
public abstract class AbstractLeaseTest {

    @Autowired
    protected LeaseCoordinator coordinator;
    @Autowired
    protected LeaseProperties properties;
    @Autowired
    protected ObjectMapper mapper;
    @Autowired
    protected MockMvc mockMvc;
    @Autowired
    protected SystemClock clockBean;

    protected ControllableClock clock;
    protected FileLeaseStore leaseStore;
    protected FileIdempotencyStore idempotencyStore;
    protected WriteLedger writeLedger;
    protected Path dataDir;

    @BeforeEach
    void wireFreshStores() throws Exception {
        clock = (ControllableClock) clockBean;
        dataDir = Files.createTempDirectory("lease-it-");
        properties.setDataDir(dataDir.toString());

        leaseStore = new FileLeaseStore(properties, mapper, clock);
        idempotencyStore = new FileIdempotencyStore(properties, mapper);
        writeLedger = new WriteLedger(properties);
        inject(coordinator, "leaseStore", leaseStore);
        inject(coordinator, "idempotencyStore", idempotencyStore);
        inject(coordinator, "writeLedger", writeLedger);
    }

    @AfterEach
    void deleteDataDir() throws IOException {
        if (dataDir == null) {
            return;
        }
        try (var paths = Files.walk(dataDir)) {
            paths.sorted(Comparator.reverseOrder()).forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                } catch (IOException ignored) {
                }
            });
        }
    }

    private static void inject(Object target, String fieldName, Object value) throws Exception {
        Field f = target.getClass().getDeclaredField(fieldName);
        f.setAccessible(true);
        f.set(target, value);
    }
}
