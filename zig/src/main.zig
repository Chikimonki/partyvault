const std = @import("std");
const crypto = std.crypto;
const mem = std.mem;

pub fn main() !void {
    // --- stdout: new buffered writer ---
    var out_buffer: [4096]u8 = undefined;
    var out_writer = std.fs.File.stdout().writer(&out_buffer);
    const stdout = &out_writer.interface;

    // --- parse command line ---
    var args = std.process.args();
    _ = args.skip();
    const mode = args.next() orelse "process";

    if (mem.eql(u8, mode, "keygen")) {
        std.debug.print("[ZIG/CRYPTO] Generating Ed25519 keypair...\n", .{});
        var keypair = std.crypto.sign.Ed25519.KeyPair.generate();
        try stdout.print("KEYPAIR|public=", .{});
        for (keypair.public_key.toBytes()) |b| {
            try stdout.print("{x:0>2}", .{b});
        }
        try stdout.print("|secret=REDACTED\n", .{});
        try stdout.flush();
    } else if (mem.eql(u8, mode, "version")) {
        try stdout.print("PartyVault Crypto Core v0.1.0 (Zig)\n", .{});
        try stdout.flush();
    } else if (mem.eql(u8, mode, "process")) {
        std.debug.print("[ZIG/CRYPTO] Processing party records...\n", .{});
        try stdout.print("# PartyVault Cryptographic Identity Output\n", .{});
        try stdout.flush();

        // --- stdin: new reader with takeDelimiterInclusive ---
        var stdin_buffer: [4096]u8 = undefined;
        var stdin_reader = std.fs.File.stdin().reader(&stdin_buffer);
        const reader = &stdin_reader.interface;

        var line_num: usize = 0;

        while (true) {
            // Read one line including newline (uses internal buffer)
            const line = reader.takeDelimiterInclusive('\n') catch |err| switch (err) {
                error.EndOfStream => break,
                else => {
                    std.debug.print("[ZIG/CRYPTO] Read error: {s}\n", .{@errorName(err)});
                    break;
                },
            };
            line_num += 1;

            // Skip header line
            if (line_num == 1) continue;

            // Remove trailing newline (takeDelimiterInclusive includes it)
            if (line.len == 0) continue;
            const record = if (line[line.len-1] == '\n') line[0..line.len-1] else line;
            if (record.len == 0) continue;

            // Simple CSV parsing (assumes no quoted commas)
            var fields: [10][]const u8 = .{""} ** 10;
            var field_count: usize = 0;
            var start: usize = 0;
            for (record, 0..) |c, i| {
                if (c == ',' and field_count < 10) {
                    fields[field_count] = record[start..i];
                    field_count += 1;
                    start = i + 1;
                }
            }
            if (field_count < 10) {
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
            try stdout.flush();
        }

        std.debug.print("[ZIG/CRYPTO] Processed {d} lines.\n", .{line_num});
    } else {
        std.debug.print("Usage: partyvault-crypto [process|keygen|version]\n", .{});
        std.process.exit(1);
    }
}
