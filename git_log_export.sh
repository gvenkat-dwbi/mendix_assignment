git --git-dir mendix_assignment/.git log --name-only \
   --pretty=format:'%n{%n  "commit": "%H",%n  "author": "%an <%ae>",%n  "date": "%ad",%n  "message": "%f":FILES:' \
   --date=iso \
   --after="2022-08-20" \
   --before="2022-08-25" \
   $@ | \
       perl -ne '
           BEGIN{print "["};
           if ($i = (/:FILES:[\n\r]*$/../^$/)) {
               if ($i == 1) {
                   s/:FILES:[\n\r]*$//;
                   $message = $_;
               } elsif ($i =~ /E0$/) {
                   #$print_files->();
                   print_files();
                   @files = ();
               } elsif ($_ !~ /^$/) {
                   chomp $_;
                   push @files, $_;
               }
           } else { print; }
           END { print_files(1); }

           sub print_files {
               $last_line = shift;
               print $message;
               @files ?
                   print qq(,\n  "files": [\n@{[join qq(,\n), map {qq(    "$_")} map {json_escape($_)} @files]}\n  ]\n})
                   : print "\n}";
               $last_line ? print "]" : print @files ? "," : ",\n";
           };
           sub json_escape { $_ = shift; s/([\\"])/\\\1/g; return $_; }' \
      > git_commit_log_2022-08-23.json
