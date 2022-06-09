library(ggpubr)

agg_df <- read.csv("src/data/genomics.csv")

fig1 <- ggscatter(agg_df, x = "nextstrain_genome_count", y = "Gh_confirmed_cases", 
                  color="#3182BD",
                  add = "reg.line", conf.int = TRUE, 
                  #cor.coef = TRUE, cor.method = "pearson",
                  xlab = "Number of MPXV genomes", ylab = "Number of Confirmed Cases",
                  label = "Country", repel = TRUE,
                  font.label = "black") +
  stat_cor(method = "pearson", label.x = 0, label.y = max(agg_df$Gh_confirmed_cases)+30) +
  coord_cartesian(clip = 'off') +
  theme(text = element_text(colour = "black"))

png("build/figures/genomics.png",
    width=15, height=15, units="cm", res=500)
print(fig1)
dev.off()
