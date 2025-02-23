#!/bin/bash

# Number of tests
tests_count=100

# Input and Output files
infile="./in.txt"
outfile="./out.txt"

# Function to generate a file with sequential numbers
genfile() {
    lines=$1
    : > "$infile"  # Clear the file
    seq 1 "$lines" > "$infile"
}

# Function to check file checksums
check_cksum() {
    in_checksum=$(cksum "$infile" | awk '{print $1}')
    out_checksum=$(cksum "$outfile" | awk '{print $1}')
    [ "$in_checksum" = "$out_checksum" ] && return 0 || return 1
}

# Run the tests
for i in $(seq 1 "$tests_count"); do
    genfile "$i"

    go run fileStreamer.go -listen &
    sleep 25
    go run fileStreamer.go -send 

    # Call function and check return status
    check_cksum
    if [ $? -eq 1 ]; then
        echo "Test ${i} failed ❌"
        exit 1
    else 
        echo "Test ${i} completed ✅"
    fi
    echo "======================================================================================="
done
