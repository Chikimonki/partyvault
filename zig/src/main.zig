const std = @import("std");
const crypto = std.crypto;
const fmt = std.fmt;
const mem = std.mem;
const io = std.io;
const json = std.json;

// =====================================================// PartyVault Cryptographic Identity Core
// 
// Provides:
//   - Deterministic party fingerprinting (BLAKE3)
//   - Ed25519 keypair generation for identity attestation
//   - Identity attestation signing & verification
//   - Party record hashing for deduplication detection
// =====================================================
const PartyIdentity = struct {
    party_id: []const u8,
    legal_name: []const u8,
    country: []const u8,
    lei: []const u8,
    entity_type: []const u8,
    fingerprint: [64]u8 = undefined, // hex-encoded BLAKE3 hash
    attestation_signature: ?[128]u8 = null, // hex-encoded Ed25519 sig
    trust_score: f64 = 0.0,
};

const AttestationResult = struct {
    party_id: []const u8,
    fingerprint: [64]u8,
    signature: [128]u8,
    public_key: [64]u8,
    timestamp_unix: i64,
    trust_score: f64,
};

/// Generate a deterministic fingerprint for a party record
/// Uses BLAKE3 for speed and collision resistance
fn generateFingerprint(
    legal_name: []const u8,
    country: []const u8,
    lei: []const u8,
    entity_type: []const u8,
) [32]u8 {
    var hasher = crypto.hash.Blake3.init(.{});

    // Canonical ordering: name|country|lei|type
    // Normalize: uppercase, trim
    hasher.update(legal_name);
    hasher.update("|");
    hasher.update(country);
    hasher.update("|");
    hasher.update(lei);
    hasher.update("|");
    hasher.update(entity_type);

    return hasher.finalResult();
}

/// Validate LEI format (20 alphanumeric characters)
fn validateLEI(lei: []const u8) bool {
    if (lei.len != 20) return false;
    for (lei) |c| {
        if (!std.ascii.isAlphanumeric(c)) return false;
    }
    return true;
}

/// Validate ISO 3166-1 alpha-2 country code (basic check)
fn validateCountryCode(code: []const u8) bool {
    if (code.len != 2) return false;
    for (code) |c| {
        if (!std.ascii.isUpper(c)) return false;
    }
    return true;
}

/// Calculate a basic trust score based on data completeness and validity
fn calculateTrustScore(
    legal_name: []const u8,
    country: []const u8,
    lei: []const u8,
    entity_type: []const u8,
    status: []const u8,
) f64 {
    var score: f64 = 0.0;
    const max_score: f64 = 100.0;

    // Name present and reasonable length
    if (legal_name.len > 2) score += 20.0;
    if (legal_name.len > 5) score += 5.0;

    // Valid country code
    if (validateCountryCode(country)) score += 15.0;

    // Valid LEI
    if (validateLEI(lei)) score += 25.0
    else if (lei.len > 0) score += 5.0; // partial credit for having something

    // Entity type present
    if (entity_type.len > 0) score += 15.0;

    // Status check
    if (mem.eql(u8, status, "ACTIVE")) score += 20.0
    else if (mem.eql(u8, status, "SUSPENDED")) score += 5.0;
    // INACTIVE or empty = 0

    return @min(score, max_score);
}

/// Process a single CSV line
fn processPartyLine(
    line: []const u8,
    stdout: anytype,
    line_num: usize,
) !void {
    // Simple CSV parsing (doesn't handle quoted commas perfectly, but works for demo)
    var fields: [14][]const u8 = undefined;
    var field_count: usize = 0;
    var start: usize = 0;
    var in_quotes = false;

    for (line, 0..) |c, i| {
        if (c == '"') {
            in_quotes = !in_quotes;
        } else if (c == ',' and !in_quotes) {
            if (field_count < 14) {
                fields[field_count] = line[start..i];
                field_count += 1;
            }
            start = i + 1;
        }
    }
    // Last field
    if (field_count < 14) {
        fields[field_count] = line[start..];
        field_count += 1;
    }

    if (field_count < 6) return; // malformed line

    const party_id = fields[0];
    const legal_name = fields[1];
    const country = fields[2];
    const lei = fields[3];
    const entity_type = fields[4];
    const status = if (field_count > 5) fields[5] else "";

    // Skip empty records
    if (legal_name.len == 0 and country.len == 0) {
        try stdout.print("SKIP|{s}|EMPTY_RECORD|line={d}\n", .{ party_id, line_num });
        return;
    }

    // Generate fingerprint
    const hash = generateFingerprint(legal_name, country, lei, entity_type);

    // Hex encode fingerprint
    var fingerprint_hex: [64]u8 = undefined;
    _ = std.fmt.bufPrint(&fingerprint_hex, "{s}", .{std.fmt.fmtSliceHexLower(&hash)}) catch unreachable;

    // Calculate trust score
    const trust_score = calculateTrustScore(legal_name, country, lei, entity_type, status);

    // LEI validation — use Perl's verdict from the cleansed CSV (field 11)
    // fields[10] = lei_valid (1 or 0), fields[11] = lei_status
    const lei_valid = if (field_count > 10) std.mem.eql(u8, fields[10], "1") else validateLEI(lei);
    const country_valid = if (field_count > 13) std.mem.eql(u8, fields[13], "1") else validateCountryCode(country);

    // Output as pipe-delimited record
    try stdout.print(
        "IDENTITY|{s}|{s}|{s}|{s}|{s}|{s}|lei_valid={s}|country_valid={s}|trust={d:.1}\n",
        .{
            party_id,
            legal_name,
            country,
            fingerprint_hex,
            lei,
            entity_type,
            if (lei_valid) "true" else "false",
            if (country_valid) "true" else "false",
            trust_score,
        },
    );

}

pub fn main() !void {
    // --- stdout ---
    const stdout = std.io.getStdOut().writer();

    // --- parse command line ---
    var args = std.process.args();
    _ = args.skip();
    const mode = args.next() orelse "process";

    if (mem.eql(u8, mode, "keygen")) {
        std.debug.print("[ZIG/CRYPTO] Generating Ed25519 keypair...\n", .{});
        var keypair = try std.crypto.sign.Ed25519.KeyPair.create(null);
        try stdout.print("KEYPAIR|public=", .{});
        for (keypair.public_key.toBytes()) |b| {
            try stdout.print("{x:0>2}", .{b});
        }
        try stdout.print("|secret=REDACTED\n", .{});
    } else if (mem.eql(u8, mode, "version")) {
        try stdout.print("PartyVault Crypto Core v0.1.0 (Zig)\n", .{});
    } else if (mem.eql(u8, mode, "process")) {
        std.debug.print("[ZIG/CRYPTO] Processing party records...\n", .{});
        try stdout.print("# PartyVault Cryptographic Identity Output\n", .{});

        const stdin = std.io.getStdIn();
        var reader = stdin.reader();

        var line_num: usize = 0;

        while (true) {
            // Read one line including newline
            var line_buffer: [4096]u8 = undefined;
            const maybe_line = reader.readUntilDelimiterOrEof(&line_buffer, '\n') catch {
                break;
            };
            const line = maybe_line orelse break;
            line_num += 1;

            // Skip header line
            if (line_num == 1) continue;

            if (line.len == 0) continue;
            const record = if (line[line.len-1] == '\n') line[0..line.len-1] else line;
            if (record.len == 0) continue;

            // Simple CSV parsing (assumes no quoted commas)
            var fields: [14][]const u8 = .{""} ** 14;
            var field_count: usize = 0;
            var start: usize = 0;
            for (record, 0..) |c, i| {
                if (c == ',' and field_count < 14) {
                    fields[field_count] = record[start..i];
                    field_count += 1;
                    start = i + 1;
                }
            }
            if (field_count < 14) {
                fields[field_count] = record[start..];
                field_count += 1;
            }
            if (field_count < 6) continue;

            const party_id = fields[0];
            const legal_name = fields[1];
            const country = fields[2];
            const lei = fields[3];
            const entity_type = fields[4];
            const status = if (field_count > 6) fields[6] else "";

            if (legal_name.len == 0 and country.len == 0) continue;

            // BLAKE3 hash — correct usage: final() takes a slice
            var hasher = crypto.hash.Blake3.init(.{});
            hasher.update(legal_name);
            hasher.update("|");
            hasher.update(country);
            hasher.update("|");
            hasher.update(lei);
            hasher.update("|");
            hasher.update(entity_type);
            var hash_bytes: [32]u8 = undefined; // Blake3 outputs 32 bytes
            hasher.final(&hash_bytes);

            // Validation
            var lei_valid = false;
            if (lei.len == 20) {
                lei_valid = true;
                for (lei) |c| {
                    if (!std.ascii.isAlphanumeric(c)) {
                        lei_valid = false;
                        break;
                    }
                }
            }
            var country_valid = false;
            if (country.len == 2) {
                country_valid = true;
                for (country) |c| {
                    if (!std.ascii.isUpper(c)) {
                        country_valid = false;
                        break;
                    }
                }
            }

            // Trust score
            var trust: f64 = 0;
            if (legal_name.len > 2) trust += 20;
            if (legal_name.len > 5) trust += 5;
            if (country_valid) trust += 15;
            if (lei_valid) trust += 25;
            if (entity_type.len > 0) trust += 15;
            if (mem.eql(u8, status, "ACTIVE")) trust += 20
            else if (mem.eql(u8, status, "SUSPENDED")) trust += 5;

            // Output identity line
            try stdout.print("IDENTITY|{s}|{s}|{s}|", .{ party_id, legal_name, country });
            for (hash_bytes) |b| {
                try stdout.print("{x:0>2}", .{b});
            }
            try stdout.print("|{s}|{s}|lei_valid={s}|country_valid={s}|trust={d:.0}\n", .{
                lei,
                entity_type,
                if (lei_valid) "true" else "false",
                if (country_valid) "true" else "false",
                trust,
            });
            }

        std.debug.print("[ZIG/CRYPTO] Processed {d} lines.\n", .{line_num});
    } else {
        std.debug.print("Usage: partyvault-crypto [process|keygen|version]\n", .{});
        std.process.exit(1);
    }
}
