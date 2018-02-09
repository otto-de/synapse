package de.otto.edison.eventsourcing.aws.s3;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SnapshotFileTimestampParser {

    private static final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH-mmX").withZone(ZoneOffset.UTC);


    public static Instant getSnapshotTimestamp(String filename) {
        Pattern pattern = Pattern.compile(".*-snapshot-(.*)-[0-9]*.json.zip");
        Matcher matcher = pattern.matcher(filename);
        if (matcher.matches()) {
            String dateTimeString = matcher.group(1);
            return dateTimeFormatter.parse(dateTimeString, Instant::from);
        } else {
            throw new IllegalArgumentException("Could not parse timestamp from filename " + filename);
        }

    }
}
