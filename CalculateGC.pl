#!/usr/bin/perl

# ************
# Author: J. Cristobal Vera
# email: jvera8888@gmail.com


# defaults, initializations, and constants
my $help = "\n\nCalculateGC.\n".
            "\t-i  Option: Input file one.  default is STDIN.\n".
            "\t-o  Option: Out file name. default is STDOUT.\n".
            "\t-w  Option: window size. Default=20.\n".
            "\n************\nAuthor: J. Cristobal Vera, email: jvera8888\@gmail.com\n"; 
my $usage = "\nCalculateGC -d [Input File] -o [Output File] -w [Window Size]\n";
my $infh = 'STDIN';
my $outfh = 'STDOUT';
my $i = 0;
my $window = 20;


#process command line custom script tags
my %cmds = ReturnCmds(@ARGV);
die "\n$help\n$usage\n" if ($cmds{'h'});
if ($cmds{'i'}) {
  $infh = 'IN';
  open ($infh, "<$cmds{'i'}") or die "Cannot open $cmds{'i'}: $!\n";
}
if ($cmds{'o'}) {
  $outfh = 'OUT';
  open ($outfh, ">$cmds{'o'}") or die "Cannot open $cmds{'o'}: $!\n";
}
$window = $cmds{'w'} if ($cmds{'w'});

#Description (Calculate the GC contents within 50 nts of the D3 region in Figure 5-20):
#The code works in the same logic as in Appendix 1. It takes the location and extracts the relevant sequence. In this case, it extracts 50 nucleotides from one of the four regions illustrated in Figure 5-20 and calculates the GC contents. Subsequently, it prints the ratios of GC and AT in a separate file. This code demonstrates an example of extracting 50 nucleotides from the D3 region in Figure 5-20. The code was slightly modified to extract the 50 nucleotides from the other regions.

while (my $line = <$infh>) {
  chomp $line;
  next if (!$line);
  $i += 1;
  my @line = split /\t/,$line;
  my @sequence = split //,$line[1];
  my $len = scalar @sequence;
  my $x = 0;
  my @chunk = splice(@sequence,0,$window);
  print $outfh "\nSequence:\t$line[0]\n";
  my $start = 1;
  until (scalar @sequence <= 0){
    my ($a,$t,$g,$c);
    my $n = 0;
    $x += 1;
    foreach my $nuc (@chunk){
      $n += 1;
      $a += 1 if ($nuc =~ m/a/i);
      $t += 1 if ($nuc =~ m/t/i);
      $g += 1 if ($nuc =~ m/g/i);
      $c += 1 if ($nuc =~ m/c/i);
    }
    my $perGC = (($g + $c)/$n)*100;
    print $outfh "bin$x\t$start\t$perGC\n";
    @chunk = splice(@sequence,0,$window);
    $start += $window;
  }
}
print $outfh "\nLines (sequences) read: $i\n";
print STDERR "\nLines (sequences) read: $i\n";

sub ReturnCmds{
  my (@cmds) = @_;
  my ($opt);
  my %cmds;
  foreach my $cmd (@cmds){
    if (!$opt and $cmd =~ m/^-([a-zA-Z])/) {
      $opt = $1;
    }
    elsif ($opt and $cmd =~ m/^-([a-zA-Z])/){
      $cmds{$opt} = 1;
      $opt = $1;
    }
    elsif ($opt){
      $cmds{$opt} = $cmd;
      $opt = '';
    }
  }
  $cmds{$opt} = 1 if ($opt);
  return %cmds;
}
