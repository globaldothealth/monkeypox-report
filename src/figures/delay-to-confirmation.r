# @author: tannervarrelman

library(ggplot2)
library(ggpubr)
library(RColorBrewer)
library(dplyr)


gh_data <- read.csv('src/data/yesterday.csv')
gh_data$Date_confirmation <- strptime(gh_data$Date_confirmation, format = "%Y-%m-%d")
gh_data$Date_entry <- strptime(gh_data$Date_entry, format = "%Y-%m-%d")

# assume that if date entry < date confirmation, then the row went from suspected to confirmed
# calculate the time difference in days for the status change
gh_data_delay <- gh_data %>%
  filter(Status=="confirmed") %>%
  filter(Date_entry < Date_confirmation) %>%
  mutate(confirmation_delay = difftime(Date_confirmation, Date_entry, units = "days")) %>%
  filter(confirmation_delay != 0)
# calculate the hist counts 
group_counts <- gh_data_delay %>%
  group_by(confirmation_delay) %>%
  summarise(n = n())
# use the max count to inform the position of figure insets
hist_max <- max(group_counts$n)
# median and mean confirmation delay
med <- median(gh_data_delay$confirmation_delay)
mean <- mean(gh_data_delay$confirmation_delay)

# size of the color palette depends on the number of countries
n <- length(unique(gh_data_delay$Country))
qual_col_pals = brewer.pal.info[brewer.pal.info$category == 'qual',]
col_vector = unlist(mapply(brewer.pal, qual_col_pals$maxcolors, rownames(qual_col_pals)))

delay_fig <- ggplot(gh_data_delay) +
  geom_histogram(aes(confirmation_delay, fill=Country), color='black') +
  scale_fill_manual(values = col_vector) +
  geom_vline(xintercept=med, linetype="dashed", color="black") +
  geom_vline(xintercept=mean, linetype="dashed", color="grey50") +
  annotate("text", x=med, y=hist_max+10, label=paste("Median Delay:", med), color="black", hjust = -0.11) +
  annotate("text", x=mean, y=hist_max+5, label=paste("Mean Delay:", round(mean, digits=4)), color="grey50", hjust=-0.09) +
  labs(x='Confirmation Delay (Days)', y='Count') +
  theme_classic() +
  annotate("text", x = med, y = hist_max, label = paste("Suspected \u2192 Confirmed:", nrow(gh_data_delay)), hjust=-0.05) +
  scale_x_continuous(n.breaks = 10)

png("build/figures/delay-to-confirmation.png",
    width=20, height=15, units="cm", res=500)
print(delay_fig)
dev.off()
