package com.github.h3yow.fencingleasecoordinatorjava.storage;

import com.github.h3yow.fencingleasecoordinatorjava.config.LeaseProperties;
import com.github.h3yow.fencingleasecoordinatorjava.error.StorageConflictException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

/**
 * 资源写入台账：持久化每个资源每次被接受的写入令牌，
 * 作为"旧/相同令牌不得写入成功"的第二道防线。
 */
public class WriteLedger {

    private final Path writeDir;

    public WriteLedger(LeaseProperties properties) {
        this.writeDir = Paths.get(properties.getDataDir(), "writes");
        try {
            Files.createDirectories(writeDir);
        } catch (IOException e) {
            throw new UncheckedIOException("cannot create writes dir: " + writeDir, e);
        }
    }

    private Path fileFor(String resourceId) {
        String name = URLEncoder.encode(resourceId, StandardCharsets.UTF_8).replace("+", "%20");
        return writeDir.resolve(name + ".log");
    }

    /**
     * 追加一条被接受的写入；要求令牌严格大于该资源上一次接受写入的令牌。
     */
    public synchronized void acceptWrite(String resourceId, String holderId, long token, String payload) {
        Path file = fileFor(resourceId);
        long lastToken = lastAcceptedToken(resourceId);
        if (token <= lastToken) {
            throw new StorageConflictException(
                    "write rejected by ledger: token " + token + " <= last " + lastToken
                            + " for resource " + resourceId);
        }
        String line = token + "\t" + holderId + "\t" + (payload == null ? "" : payload.replace('\n', ' '))
                + System.lineSeparator();
        try {
            Files.writeString(file, line, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new UncheckedIOException("cannot append write ledger: " + resourceId, e);
        }
    }

    /** 该资源最近一次被接受写入的令牌；无历史返回 0。 */
    public long lastToken(String resourceId) {
        return lastAcceptedToken(resourceId);
    }

    private long lastAcceptedToken(String resourceId) {
        Path file = fileFor(resourceId);
        if (!Files.exists(file)) {
            return 0L;
        }
        try {
            String content = Files.readString(file, StandardCharsets.UTF_8);
            long last = 0L;
            for (String line : content.split("\\R")) {
                if (line.isBlank()) {
                    continue;
                }
                int tab = line.indexOf('\t');
                last = Long.parseLong(tab < 0 ? line : line.substring(0, tab));
            }
            return last;
        } catch (IOException e) {
            throw new UncheckedIOException("cannot read write ledger: " + resourceId, e);
        }
    }
}
