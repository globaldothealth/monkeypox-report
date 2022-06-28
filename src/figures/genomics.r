library(ggpubr)

agg_df <- read.csv("src/data/genomics.csv")

fig1 <- ggscatter(agg_df, x = "nextstrain_genome_count", y = "Gh_confirmed_cases", 
                  color="#007AEC",
                  add = "reg.line", conf.int = TRUE, 
                  #cor.coef = TRUE, cor.method = "pearson",
                  xlab = "Number of MPXV genomes", ylab = "Number of Confirmed Cases",
                  label = "Country", repel = TRUE,
                  font.label = "black") +
  stat_cor(method = "pearson", label.y = 0, cor.coef.name = "r",
           label.x = 0.6 * max(agg_df$nextstrain_genome_count)) +
  coord_cartesian(clip = 'off') +
  theme(text = element_text(colour = "black"))

png("build/figures/genomics.png",
    width=15, height=15, units="cm", res=500)
print(fig1)
dev.off()
