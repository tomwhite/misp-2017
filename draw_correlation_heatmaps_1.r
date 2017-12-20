
#####
##best heatmap so far.
##leaves non-significant correlations "blank"
#####

library("Hmisc")
library("corrplot")

#get count matrix from file
mat_p12_unfiltered <- read.table("C:/Users/jcvera/Desktop/MISP/testout_p12_unfiltered.txt", header=TRUE,row.names=1)

#use rcorr from Hmisc package
rcormat <- rcorr(as.matrix(mat_p12_unfiltered))

corrplot(rcormat$r, method="color", col=col(200),  
         type="upper", order="hclust", 
         addCoef.col = "black", # Add coefficient of correlation
         tl.col="black", tl.srt=45, #Text label color and rotation
         # Combine with significance
         p.mat = rcormat$P, sig.level = 0.05, insig = "blank", 
         # hide correlation coefficient on the principal diagonal
         diag=FALSE 
)
