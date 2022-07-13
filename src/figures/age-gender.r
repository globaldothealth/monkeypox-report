# @author: tannervarrelman

library(ggplot2)
library(stringr)
library(dplyr)

age_bins <- function(age, count){
  bins <- c(list(c(0,10)),
            list(c(11,20)),
            list(c(21,30)),
            list(c(31,40)),
            list(c(41,50)),
            list(c(51,60)),
            list(c(61,70)),
            list(c(71, 80)))
  split_age = str_split_fixed(age, "-", 2)
  start_age = as.integer(split_age[1])
  end_age = as.integer(split_age[2])
  for (i in 1:length(bins)) {
    start_bin = bins[[i]][[1]]
    end_bin = bins[[i]][[2]]
    if (start_bin <= start_age &  start_age <= end_bin){
      start_index <- i
    }
    if (start_bin <= end_age & end_age <= end_bin){
      end_index <-i
    }
  }
  index_list <- seq(start_index, end_index, by=1)
  return(list(index_list))
}

gh_data <- read.csv('src/data/yesterday.csv')
gh_data$Gender <- trimws(tolower(gh_data$Gender))

con_df <- gh_data %>%
  filter(Status=='confirmed') %>%
  filter(Age!='<40' & Age!='' & Gender!='') %>%
  group_by(Age, Gender) %>%
  summarise(n=n()) %>%
  rowwise() %>% 
  mutate(bin_index = age_bins(Age, n))

bin_names <- c('0-10', '11-20', '21-30', '31-40', '41-50', '51-60', '61-70', '71-80', '80+')
transformed_bins <- data.frame()

for(i in 1:nrow(con_df)){
  bin_list <- con_df$bin_index[i]
  gend <- con_df$Gender[i]
  distributed_n <- (con_df$n[i])/(length(bin_list[[1]]))
  for(bin in bin_list){
    bin_name <- bin_names[bin]
    bin_df <- data.frame(Age = bin_name, Gender = gend, n = distributed_n)
    transformed_bins <- rbind(transformed_bins, bin_df)
  }
}

final_bins <- transformed_bins %>%
  group_by(Age, Gender) %>%
  summarise(total = sum(n)) %>%
  rowwise() %>%
  add_row(Age = setdiff(bin_names, transformed_bins[transformed_bins$Gender=='male', ]$Age), total = 0, Gender='male') %>%
  add_row(Age = setdiff(bin_names, transformed_bins[transformed_bins$Gender=='female', ]$Age), total = 0, Gender='female')
  
final_bins$Age <- factor(final_bins$Age, levels=bin_names)
max_n <- max(final_bins$total)

pop_pyramid <- ggplot(data = final_bins, 
        mapping = aes(x = ifelse(test = Gender == "male", yes = -total, no = total), 
                      y = Age, fill = Gender)) +
    scale_x_continuous(labels = abs, limits = c(-max_n, max_n)) +
    geom_col(show.legend=FALSE, color='black', size=0.05) +
    labs(x = "Number of Cases", y="Age", title="Male           Female") +
    theme_classic() +
    scale_fill_manual(values=c("#007AEC", "#6BADEA")) +
    theme(plot.title = element_text(hjust = 0.5))

png('build/figures/age-gender.png',
    width=15, height=7.5, units="cm", res=500)
print(pop_pyramid)
dev.off()
