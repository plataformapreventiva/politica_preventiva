
dbSafeNames = function(names) {
  names = gsub('[^a-z0-9]+','_',tolower(names))
  names = make.names(names, unique=TRUE, allow_=TRUE)
  names = gsub('.','_',names, fixed=TRUE)
  names
}

normalize <- function(x) {
  return ((x - min(x)) / (max(x) - min(x)))
}

scale <- function(x) {
  return ((x - min(x)) / (max(x) - min(x)))
}

std <- function(x){
  if(length(which(is.na(x)))==0) (x-mean(x))/sd(x) else
    (x-mean(x,na.rm=T))/sd(x,na.rm=T)
}
