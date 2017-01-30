dbSafeNames = function(names) {
  names = gsub('_$','',names)
  names = gsub('[^a-z0-9]+','_',tolower(names))
  names[grepl("^[0-9]",names)] = paste0("p_",names[grepl("^[0-9]",names)])
  names = make.names(names, unique=TRUE, allow_=TRUE)
  names = gsub('.','_',names, fixed=TRUE)
  names
}

normalize <- function(x) {
  return ((x - min(x)) / (max(x) - min(x)))
}


standardize <- function(x){
  if(length(which(is.na(x)))==0) (x-mean(x))/sd(x) else
    (x-mean(x,na.rm=T))/sd(x,na.rm=T)
}
