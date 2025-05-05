#!/usr/bin/env fish

set src_dir streaming
set dest_dir source
set interval 2

while true
    set file (find $src_dir -maxdepth 1 -type f | head -n 1)
    if test -n "$file"
        mv "$file" "$dest_dir"
        echo "Moved (basename $file)"
    else
        echo "No more files to move. Exiting."
        break
    end
    sleep $interval
end
