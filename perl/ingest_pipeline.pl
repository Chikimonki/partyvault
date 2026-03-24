#!/usr/bin/env perl
# ============================================================
# PartyVault Data Ingestion Pipeline
# 
# Ingests party data from multiple formats (CSV, JSON),
# cleanses, normalizes, validates, and outputs standardized
# records ready for cryptographic fingerprinting.
#
# This is where Perl SHINES — text munging, regex, ETL.
# ============================================================

use strict;
use warnings;
use utf8;
use Digest::SHA qw(sha256_hex);
use JSON;
use File::Basename;
use Getopt::Long;
use POSIX qw(strftime);

binmode(STDOUT, ":utf8");
binmode(STDERR, ":utf8");

# --- Configuration ---
my $data_dir = "./data";
my $output_file = "./output/cleansed_parties.csv";
my $report_file = "./output/ingestion_report.txt";
my $verbose = 0;

GetOptions(
    "data-dir=s"  => \$data_dir,
    "output=s"    => \$output_file,
    "report=s"    => \$report_file,
    "verbose"     => \$verbose,
) or die "Usage: $0 [--data-dir DIR] [--output FILE] [--verbose]\n";

# --- Statistics ---
my %stats = (
    total_records    => 0,
    valid_records    => 0,
    cleansed_records => 0,
    rejected_records => 0,
    duplicates_found => 0,
    files_processed  => 0,
);

my @issues = ();
my @all_records = ();
my %seen_fingerprints = ();

# --- Country code validation (EU/EEA + common financial centers) ---
my %valid_countries = map { $_ => 1 } qw(
    AT BE BG CY CZ DE DK EE ES FI FR GR HR HU IE IT LT LU LV
    MT NL PL PT RO SE SI SK GB CH NO IS LI US CA JP AU SG HK
    KY BS BM VG JE GG IM
);

# --- Entity type normalization ---
my %entity_type_map = (
    'CREDIT_INSTITUTION'  => 'CREDIT_INSTITUTION',
    'BANK'                => 'CREDIT_INSTITUTION',
    'INVESTMENT_FIRM'     => 'INVESTMENT_FIRM',
    'FUND'                => 'INVESTMENT_FUND',
    'INVESTMENT_FUND'     => 'INVESTMENT_FUND',
    'CSD'                 => 'CSD',
    'ICSD'                => 'CSD',
    'EXCHANGE'            => 'EXCHANGE',
    'SHELL_COMPANY'       => 'SHELL_COMPANY',
    'INSURANCE'           => 'INSURANCE_UNDERTAKING',
    'CORPORATE'           => 'NON_FINANCIAL_CORPORATE',
);

# ============================================================
# CLEANSING FUNCTIONS — Perl's regex power on full display
# ============================================================

sub normalize_name {
    my ($name) = @_;
    return '' unless defined $name && $name =~ /\S/;
    
    # Trim whitespace
    $name =~ s/^\s+|\s+$//g;
    
    # Collapse multiple spaces
    $name =~ s/\s{2,}/ /g;
    
    # Normalize common legal entity suffixes
    $name =~ s/\bS\.A\.\s*/SA /gi;
    $name =~ s/\bS\.A\.\/N\.V\./SA\/NV/gi;
    $name =~ s/\bN\.V\.\s*/NV /gi;
    $name =~ s/\bA\.G\.\s*/AG /gi;
    $name =~ s/\bGmbH\b/GmbH/gi;
    $name =~ s/\bLtd\.?\b/Ltd/gi;
    $name =~ s/\bLimited\b/Ltd/gi;
    $name =~ s/\bP\.?L\.?C\.?\b/PLC/gi;
    $name =~ s/\bInc\.?\b/Inc/gi;
    $name =~ s/\bCorp\.?\b/Corp/gi;
    $name =~ s/\bAbp\.?\b/Abp/gi;
    
    # Trim again after substitutions
    $name =~ s/\s+$//;
    
    return $name;
}

sub normalize_address {
    my ($addr) = @_;
    return '' unless defined $addr && $addr =~ /\S/;
    
    $addr =~ s/^\s+|\s+$//g;
    $addr =~ s/^["']|["']$//g;  # Remove surrounding quotes
    $addr =~ s/\s{2,}/ /g;
    
    # Normalize common abbreviations
    $addr =~ s/\bStr\.\s*/Street /gi;
    $addr =~ s/\bBlvd\.?\s*/Boulevard /gi;
    $addr =~ s/\bAve\.?\s*/Avenue /gi;
    
    return $addr;
}

sub validate_lei {
    my ($lei) = @_;
    return { valid => 0, reason => 'MISSING' } unless defined $lei && $lei =~ /\S/;
    
    # LEI is exactly 20 alphanumeric characters
    unless ($lei =~ /^[A-Z0-9]{20}$/) {
        return { valid => 0, reason => 'INVALID_FORMAT' };
    }
    
    # Basic checksum validation (ISO 17442 uses MOD 97-10)
    # Convert letters to numbers (A=10, B=11, ..., Z=35)
    my $numeric = '';
    for my $c (split //, $lei) {
        if ($c =~ /[A-Z]/) {
            $numeric .= (ord($c) - ord('A') + 10);
        } else {
            $numeric .= $c;
        }
    }
    
    # MOD 97 check
    my $remainder = 0;
    for my $digit (split //, $numeric) {
        $remainder = ($remainder * 10 + $digit) % 97;
    }
    
    if ($remainder != 1) {
        return { valid => 0, reason => 'CHECKSUM_FAIL' };
    }
    
    return { valid => 1, reason => 'OK' };
}

sub validate_email {
    my ($email) = @_;
    return 0 unless defined $email && $email =~ /\S/;
    return $email =~ /^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$/;
}

sub generate_dedup_key {
    my ($name, $country, $lei) = @_;
    # Normalized deduplication key
    my $key = lc(normalize_name($name // ''));
    $key =~ s/\s+//g;  # Remove all whitespace for fuzzy matching
    $key .= '|' . uc($country // '') . '|' . uc($lei // '');
    return sha256_hex($key);
}

# ============================================================
# FILE PARSERS
# ============================================================

sub parse_csv_file {
    my ($filename) = @_;
    my @records;
    
    open(my $fh, '<:utf8', $filename) or do {
        warn "[PERL/INGEST] Cannot open $filename: $!\n";
        return @records;
    };
    
    my $header = <$fh>;
    chomp $header;
    my @fields = split /,/, $header;
    
    while (my $line = <$fh>) {
        chomp $line;
        next unless $line =~ /\S/;
        
        # Handle quoted fields with commas
        my @values;
        my $remaining = $line;
        while ($remaining =~ /("([^"]*(?:""[^"]*)*)"|([^,]*))(?:,|$)/g) {
            my $val = defined($2) ? $2 : ($3 // '');
            $val =~ s/""/"/g;
            push @values, $val;
        }
        
        my %record;
        for my $i (0 .. $#fields) {
            my $field = $fields[$i];
            $field =~ s/^\s+|\s+$//g;
            $record{$field} = $values[$i] // '';
        }
        $record{_source_file} = basename($filename);
        $record{_source_format} = 'CSV';
        
        push @records, \%record;
    }
    
    close $fh;
    return @records;
}

sub parse_json_file {
    my ($filename) = @_;
    my @records;
    
    open(my $fh, '<:utf8', $filename) or do {
        warn "[PERL/INGEST] Cannot open $filename: $!\n";
        return @records;
    };
    
    my $content = do { local $/; <$fh> };
    close $fh;
    
    my $data = decode_json($content);
    $data = [$data] unless ref $data eq 'ARRAY';
    
    for my $record (@$data) {
        $record->{_source_file} = basename($filename);
        $record->{_source_format} = 'JSON';
        push @records, $record;
    }
    
    return @records;
}

# ============================================================
# MAIN PIPELINE
# ============================================================

print STDERR "[PERL/INGEST] PartyVault Data Ingestion Pipeline\n";
print STDERR "[PERL/INGEST] Data directory: $data_dir\n";
print STDERR "[PERL/INGEST] Output: $output_file\n";
print STDERR "[PERL/INGEST] " . strftime("%Y-%m-%d %H:%M:%S", localtime) . "\n";
print STDERR "[PERL/INGEST] ---\n";

# Phase 1: Ingest all files
my @raw_records;

for my $file (glob("$data_dir/*.csv")) {
    print STDERR "[PERL/INGEST] Ingesting CSV: $file\n";
    push @raw_records, parse_csv_file($file);
    $stats{files_processed}++;
}

for my $file (glob("$data_dir/*.json")) {
    print STDERR "[PERL/INGEST] Ingesting JSON: $file\n";
    push @raw_records, parse_json_file($file);
    $stats{files_processed}++;
}

$stats{total_records} = scalar @raw_records;
print STDERR "[PERL/INGEST] Ingested $stats{total_records} raw records from $stats{files_processed} files.\n";

# Phase 2: Cleanse, validate, deduplicate
open(my $out_fh, '>:utf8', $output_file) or die "Cannot write $output_file: $!\n";
print $out_fh "id,legal_name,country,lei,entity_type,incorporation_date,status,address,contact_email,annual_revenue_eur,lei_valid,lei_status,email_valid,country_valid,dedup_key,source_file,cleansing_notes\n";

for my $raw (@raw_records) {
    my @notes;
    
    my $id          = $raw->{id} // '';
    my $legal_name  = $raw->{legal_name} // '';
    my $country     = $raw->{country} // '';
    my $lei         = $raw->{lei} // '';
    my $entity_type = $raw->{entity_type} // '';
    my $inc_date    = $raw->{incorporation_date} // '';
    my $status      = $raw->{status} // '';
    my $address     = $raw->{address} // '';
    my $email       = $raw->{contact_email} // '';
    my $revenue     = $raw->{annual_revenue_eur} // '';
    my $source      = $raw->{_source_file} // 'unknown';
    
    # Reject empty records
    if ($legal_name !~ /\S/ && $country !~ /\S/ && $lei !~ /\S/) {
        push @issues, "$id: REJECTED — empty record";
        $stats{rejected_records}++;
        next;
    }
    
    # Cleanse name
    my $clean_name = normalize_name($legal_name);
    if ($clean_name ne $legal_name && $legal_name =~ /\S/) {
        push @notes, "name_normalized";
        $stats{cleansed_records}++;
    }
    
    # Cleanse address
    my $clean_address = normalize_address($address);
    if ($clean_address ne $address && $address =~ /\S/) {
        push @notes, "address_normalized";
    }
    
    # Normalize country code
    $country = uc($country);
    my $country_valid = exists $valid_countries{$country} ? 1 : 0;
    unless ($country_valid) {
        push @notes, "country_invalid_or_missing";
    }
    
    # Normalize entity type
    my $norm_type = $entity_type_map{uc($entity_type)} // $entity_type;
    if ($norm_type ne $entity_type && $entity_type =~ /\S/) {
        push @notes, "entity_type_normalized";
    }
    
    # Validate LEI
    my $lei_result = validate_lei($lei);
    my $lei_valid  = $lei_result->{valid};
    my $lei_status = $lei_result->{reason};
    unless ($lei_valid) {
        push @notes, "lei_$lei_status";
    }
    
    # Validate email
    my $email_valid = validate_email($email) ? 1 : 0;
    
    # Flag high-risk patterns
    if ($norm_type eq 'SHELL_COMPANY') {
        push @notes, "HIGH_RISK:shell_company";
    }
    if ($status eq 'SUSPENDED') {
        push @notes, "HIGH_RISK:suspended_entity";
    }
    if ($country =~ /^(KY|BS|BM|VG|JE|GG|IM)$/ && !$lei_valid) {
        push @notes, "HIGH_RISK:offshore_no_lei";
    }
    
    # Deduplication
    my $dedup_key = generate_dedup_key($clean_name, $country, $lei);
    if (exists $seen_fingerprints{$dedup_key}) {
        push @notes, "DUPLICATE:matches_$seen_fingerprints{$dedup_key}";
        $stats{duplicates_found}++;
    } else {
        $seen_fingerprints{$dedup_key} = $id;
    }
    
    # Output cleansed record
    my $notes_str = join(';', @notes) || 'clean';
    
    # Escape commas in fields
    for my $field ($clean_name, $clean_address, $notes_str) {
        if ($field =~ /[,"]/) {
            $field =~ s/"/""/g;
            $field = "\"$field\"";
        }
    }
    
    print $out_fh join(',',
        $id, $clean_name, $country, $lei, $norm_type, $inc_date,
        $status, $clean_address, $email, $revenue,
        $lei_valid, $lei_status, $email_valid, $country_valid,
        substr($dedup_key, 0, 16), $source, $notes_str
    ) . "\n";
    
    $stats{valid_records}++;
    
    if ($verbose) {
        print STDERR "[PERL/INGEST]   $id: $clean_name [$notes_str]\n";
    }
}

close $out_fh;

# Phase 3: Generate report
open(my $rpt_fh, '>:utf8', $report_file) or warn "Cannot write report: $!\n";

my $report = <<"REPORT";
===============================================
  PartyVault Ingestion Report
  Generated: @{[strftime("%Y-%m-%d %H:%M:%S", localtime)]}
===============================================

Files Processed:     $stats{files_processed}
Total Records:       $stats{total_records}
Valid Records:       $stats{valid_records}
Cleansed Records:    $stats{cleansed_records}
Rejected Records:    $stats{rejected_records}
Duplicates Found:    $stats{duplicates_found}

Data Quality Score:  @{[sprintf("%.1f%%", ($stats{valid_records} / ($stats{total_records} || 1)) * 100)]}

Issues:
REPORT

for my $issue (@issues) {
    $report .= "  - $issue\n";
}

$report .= "\n--- End of Report ---\n";

print $rpt_fh $report;
close $rpt_fh;

print STDERR "[PERL/INGEST] ---\n";
print STDERR "[PERL/INGEST] Pipeline complete.\n";
print STDERR "[PERL/INGEST] Valid: $stats{valid_records} | Rejected: $stats{rejected_records} | Duplicates: $stats{duplicates_found}\n";
print STDERR "[PERL/INGEST] Output: $output_file\n";
print STDERR "[PERL/INGEST] Report: $report_file\n";

# Also output to stdout for piping to Zig
open(my $out_read, '<:utf8', $output_file) or die "Cannot read output: $!\n";
while (<$out_read>) {
    print STDOUT $_;
}
close $out_read;
