install.packages('https://cran.r-project.org/src/contrib/Archive/read.dbc/read.dbc_1.0.5.tar.gz', repos = NULL)
library(read.dbc)

base_file_name <- Sys.getenv("BASE_FILE_NAME")
print(base_file_name)

dbc_file_path <- paste(base_file_name, "dbc", sep = ".")
print(dbc_file_path)
csv_file_path <- paste(base_file_name, "csv", sep = ".")
print(csv_file_path)

x <- read.dbc(dbc_file_path)
write.csv(x, csv_file_path, row.names = FALSE)
