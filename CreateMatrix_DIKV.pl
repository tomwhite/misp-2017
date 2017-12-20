#!/usr/bin/perl

#use strict;
use Cwd;
use File::Spec; 

# defaults, initializations, and constants
my $help = "\nCreateMatrix_DIpar50\nCreates a matrix of read counts from RSEM result files.\n".
          "\nOptions-Switches:\n".
          "\t-d  Option: specify 'Unaligned/' directory with sample folders.\n".
          "\t-o  Option: specify output file name. Optional, default=STDOUT.\n".
          "\n************\nAuthor: J. Cristobal Vera, email: jcvera\@illinois.edu\n";
my $usage = "\nUsage:\nCreateMatrix_DIpar50.pl -d [Input Sample Directory]\n";
my $n = 0;
my ($indir,$title);
my $outfh = 'OUT';
my $rf = 5; #read count filter min
my $sf = 1; #mapping score filter min
my $lf = 1; #DG minimum length filter
my $na = '';  #missing value symbol; use NA for R and '' for excel or JMP
my %rows;

#process command line custom script tags
my %cmds = ReturnCmds(@ARGV);
die "\n$help\n$usage\n" if ($cmds{'h'});
$indir = $cmds{'d'} if ($cmds{'d'});
if ($cmds{'o'}) {
  $outfh = 'OUT';
  open ($outfh, ">$cmds{'o'}") or die "Cannot create $cmds{'o'}: $!\n";
}

#make absolute paths
$indir = File::Spec->rel2abs($indir) or die "\nUnknown directory path.\n";

#get all file names
opendir(INDIR,$indir) or die "Can't open directory: $indir: $!\n";
my @files = grep {m/.+.csv$/ && -f "$indir/$_"} readdir(INDIR);
close(INDIR);
@files = sort @files;
my @samples;

foreach my $file (@files){
  my $filedir = "$indir/$file";
  my $sample = $file;
  $sample =~ s/_del_sort\.csv$//;
  $sample =~ s/_chikv.+//;   ####edit this for different experiments
  push @samples,$sample;
  $n += 1;
  $title = "DIs\t$sample\_$n" if ($n == 1);
  $title .= "\t$sample\_$n" if ($n > 1);
  open (IN, "<$filedir") or die "Cannot open $filedir: $!\n";
  while (my $line = <IN>){
    chomp $line;
    my @line = split /,/,$line;
    my ($start,$stop,$cov,$reads,$score) = ($line[0],$line[1],$line[2],$line[3],$line[4]);
    my $length = $stop - $start;
    if ($length >= $lf){
      my $id = "$start\_$stop";
      $rows{$id}{$sample} = $reads;
    }
  }
  close(IN);
}
print STDERR "\nSample files parsed: $n\n\n";
@samples = sort @samples;

#print out
my $r = my $c = 0;
print $outfh "$title\n";
foreach my $id (sort keys %rows){
  $r += 1;
  print $outfh "$id";
  foreach my $sample (@samples){
    $c += 1;
    print $outfh "\t$rows{$id}{$sample}" if (exists $rows{$id}{$sample});
    print $outfh "\t$na" if (!exists $rows{$id}{$sample});
  }
  print $outfh "\n";
  $c = 0;
}

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

sub CheckJobs{
    my (@jobinfo) = @_;
    foreach my $job (@jobinfo){
        if ($job =~ m/\/(NGS[0-9]+)\.job/) {
            my $id = $1;
            return 0 if (!-e "$id.b.err");
        }
        else{
            die "\nError: job ID not found.\n";
        }
    }
    return 1;
}
