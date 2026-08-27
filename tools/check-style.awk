function count_char(text, character, copy) {
    copy = text
    return gsub(character, "", copy)
}

function finish_function(end_line, line_count, name) {
    name = function_name[function_count]
    line_count = end_line - function_start[function_count] + 1
    if (name != "Protocol" && name != "ProtocolGated" && name != "ReplicatedLog" && \
        name != "Learner" && \
        name != "Simulator" && name != "Bench" && name != "MovingBench" && FILENAME !~ /cli_test\.zig$/ && \
        FILENAME !~ /cluster_bench\.zig$/ && FILENAME !~ /cluster_test\.zig$/ && \
        FILENAME !~ /fuzz\.zig$/ && FILENAME !~ /main\.zig$/ && FILENAME !~ /node\.zig$/ && \
        FILENAME !~ /server\.zig$/ && FILENAME !~ /soak\.zig$/ && FILENAME !~ /tls\.zig$/ && line_count > 70) {
        printf "%s:%d: function %s is %d lines; maximum is 70\n", \
            FILENAME, function_start[function_count], name, line_count
        failed = 1
    }
    delete function_name[function_count]
    delete function_start[function_count]
    delete function_depth[function_count]
    function_count--
}

# A file stays below 2800 lines of code, counting neither comment-only
# lines nor blanks. The two grandfathered hosts are pinned at their
# current size and may only shrink; the pins are recorded debt, not
# policy.
function file_code_limit(name) {
    if (name ~ /zaxonlite\/src\/server\.zig$/) return 4650
    if (name ~ /zaxonlite\/src\/node\.zig$/) return 3350
    return 2800
}

{
    if ($0 !~ /^[[:space:]]*(\/\/|$)/) {
        code_lines++
    }
    if (length($0) > 100) {
        printf "%s:%d: line is %d columns; maximum is 100\n", \
            FILENAME, FNR, length($0)
        failed = 1
    }
    if (index($0, "\t") != 0) {
        printf "%s:%d: tab character is not permitted\n", FILENAME, FNR
        failed = 1
    }

    if (!pending && match($0, /(^|[[:space:]])(pub[[:space:]]+)?fn[[:space:]]+[A-Za-z_]/)) {
        pending_name = $0
        sub(/^.*fn[[:space:]]+/, "", pending_name)
        sub(/\(.*/, "", pending_name)
        pending_start = FNR
        pending = 1
    }

    opens = count_char($0, "{")
    closes = count_char($0, "}")
    if (pending && opens > 0) {
        function_count++
        function_name[function_count] = pending_name
        function_start[function_count] = pending_start
        function_depth[function_count] = brace_depth + 1
        pending = 0
    }

    brace_depth += opens - closes
    while (function_count > 0 && brace_depth < function_depth[function_count]) {
        finish_function(FNR)
    }
}

END {
    if (code_lines > file_code_limit(FILENAME)) {
        printf "%s: %d lines of code; maximum is %d (comments excluded)\n", \
            FILENAME, code_lines, file_code_limit(FILENAME)
        failed = 1
    }
    if (failed) exit 1
}
