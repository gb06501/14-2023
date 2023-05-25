#!/bin/awk -f

BEGIN {
    FS = ","
    OFS = ","
}

NR == 1 {
    # Save header row
    header_vessels = $1 OFS $8 OFS $9 OFS $10 OFS $11 OFS $13 OFS $14 OFS $17
    header_basestationreport =  $1 OFS $2 OFS $3 OFS $4 OFS $5 OFS $6 OFS $7 OFS $12 OFS $15 OFS $16 	
    next
}

{
    # Extract required columns and write to output file
    row_vessels = $1 OFS $8 OFS $9 OFS $10 OFS $11 OFS $13 OFS $14 OFS $17
    row_basestationreport = $1 OFS $2 OFS $3 OFS $4 OFS $5 OFS $6 OFS $7 OFS $12 OFS $15 OFS $16
    rows_vessels[NR] = row_vessels
    rows_basestationreport[NR] = row_basestationreport
}

END {
    # Write header and rows to output file
    outfile_vessels = substr(aisfile, 1, length(aisfile)-4) "_vesselsdup.csv"
    outfile_basestationreport = substr(aisfile, 1, length(aisfile)-4) "_basestationreport.csv"
    print header_vessels > outfile_vessels
    print header_basestationreport > outfile_basestationreport	
    for (i = 2; i <= NR; i++) {
        print rows_vessels[i] > outfile_vessels
        print rows_basestationreport[i] > outfile_basestationreport

    }
}
