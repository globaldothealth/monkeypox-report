# Code for travel history arcs and cumulative case counts for Monkeypox data
# Original-Author: Houriiyah Tegally <firstname.lastname@gmail.com>

library(dplyr)
library(cowplot)
library(rworldmap)
library(ggplot2)
library(rjson)

world_data <- getMap(resolution = "low")@data

country2continent <- function(x) {
  country_data <- subset(world_data, NAME == x)

  return(as.character(country_data[["REGION"]])) # returns the continent (7 continent model)
}


country2continent_region <- function(x) {
  country_data <- subset(world_data, NAME == x)

  return(as.character(country_data[["IMAGE24"]]))
}


country2lat <- function(x) {
  country_data <- subset(world_data, NAME == x)

  return(as.numeric(country_data[["LAT"]]))
}


country2long <- function(x) {
  country_data <- subset(world_data, NAME == x)

  return(as.numeric(country_data[["LON"]]))
}




MPXV_cases_data <- read.csv("src/data/yesterday.csv")
MPXV_cases_data <- subset(MPXV_cases_data, Status != "discarded")


MPXV_cases_data_0<-subset(subset(MPXV_cases_data,Travel_history_country==''),Travel_history_location!='')#Artifically coding unknown country of travel to the bigger location"
MPXV_cases_data_0$Travel_history_country<-MPXV_cases_data_0$Travel_history_location #Artifically coding unknown country of travel to the bigger location"
remove <- MPXV_cases_data_0$ID#Artifically coding unknown country of travel to the bigger location"
MPXV_cases_data<-MPXV_cases_data[!MPXV_cases_data$ID %in% remove, ]#Artifically coding unknown country of travel to the bigger location"
MPXV_cases_data<-rbind(MPXV_cases_data,MPXV_cases_data_0)#Artifically coding unknown country of travel to the bigger location"


MPXV_cases_data_x <- subset(MPXV_cases_data, Country == Travel_history_country)
remove_from_travel_links <- MPXV_cases_data_x$ID


MPXV_cases_data$Country[MPXV_cases_data$Country == "England"] <- "United Kingdom"
MPXV_cases_data$Travel_history_country[MPXV_cases_data$Travel_history_country == "England"] <- "United Kingdom"

MPXV_cases_data$Travel_history_country[MPXV_cases_data$Travel_history_country == "Africa"] <- "Mali" # Artificially coding for middle-ish of West Africa
MPXV_cases_data$Travel_history_country[MPXV_cases_data$Travel_history_country == "Spain; Germany"] <- "Spain" # Artificially coding for Spain
MPXV_cases_data$Travel_history_country[MPXV_cases_data$Travel_history_country == "Spain, Singapore"] <- "Spain" # Artificially coding for Spain
MPXV_cases_data$Travel_history_country[MPXV_cases_data$Travel_history_country == "Gran Canaria"] <- "Spain" # Artificially coding for Spain
MPXV_cases_data$Travel_history_country[MPXV_cases_data$Travel_history_country == "Canary Islands"] <- "Spain" # Artificially coding for Spain

MPXV_cases_data$Travel_history_country[MPXV_cases_data$Travel_history_country == "Europe"] <- "Luxembourg" # Artificially coding for middle-ish of Europe
MPXV_cases_data$Travel_history_country[MPXV_cases_data$Travel_history_country == "Western Europe"] <- "Luxembourg" # Artificially coding for middle-ish of Europe
MPXV_cases_data$Travel_history_country[MPXV_cases_data$Travel_history_country == "West Africa"] <- "Mali" # Artificially coding for middle-ish of West Africa

MPXV_cases_data_1 <- subset(MPXV_cases_data, Travel_history_entry == "early May") # Artificially coding "early May to 5th of May
MPXV_cases_data_1$Travel_history_entry <- "2022-05-05" # Artificially coding "early May to 5th of May


MPXV_cases_data_2 <- subset(subset(MPXV_cases_data, Travel_history_country!=''), Travel_history_entry=='')
 MPXV_cases_data_2$Travel_history_entry<-"2050-01-01"  #Artificially coding missing dates to a random formatted date to act as "unknown date"
#MPXV_cases_data_2a <- subset(subset(MPXV_cases_data, Travel_history_country!=''), is.na(Travel_history_entry==''))
# MPXV_cases_data_2a$Travel_history_entry<-"2050-01-01"  #Artificially coding missing dates to a random formatted date to act as "unknown date"

remove <- MPXV_cases_data_1$ID
MPXV_cases_data <- MPXV_cases_data[!MPXV_cases_data$ID %in% remove, ]


remove <- MPXV_cases_data_2$ID
MPXV_cases_data <- MPXV_cases_data[!MPXV_cases_data$ID %in% remove, ]


MPXV_cases_data$Travel_history_entry <- as.Date(MPXV_cases_data$Travel_history_entry)

MPXV_cases_data <- rbind(MPXV_cases_data, MPXV_cases_data_1,MPXV_cases_data_2)


MPXV_cases_data$Country_lat <- lapply(MPXV_cases_data$Country, country2lat)
MPXV_cases_data$Country_long <- lapply(MPXV_cases_data$Country, country2long)



MPXV_cases_data$Travel_history_country_lat <- lapply(MPXV_cases_data$Travel_history_country, country2lat)
MPXV_cases_data$Travel_history_country_long <- lapply(MPXV_cases_data$Travel_history_country, country2long)

MPXV_cases_data$date <- as.Date(MPXV_cases_data$Date_confirmation)

MPXV_cases_data$days <- as.Date(cut(MPXV_cases_data$date, breaks = "day", start.on.monday = FALSE))
MPXV_cases_data$date1 <- as.Date(cut(MPXV_cases_data$date, breaks = "1 weeks", start.on.monday = FALSE))
MPXV_cases_data$date2 <- as.Date(cut(MPXV_cases_data$date, breaks = "2 weeks", start.on.monday = FALSE))
MPXV_cases_data$date3 <- as.Date(cut(MPXV_cases_data$date, breaks = "1 month", start.on.monday = FALSE))


df_count <- MPXV_cases_data %>% count(Country)
names(df_count)[names(df_count) == "Country"] <- "Country"
names(df_count)[names(df_count) == "n"] <- "Count"
df_count


df_count <- df_count %>%
  left_join(MPXV_cases_data, by = c("Country" = "Country"))

df_count$Country[df_count$Country == "United Kingdom"] <- "UK"
df_count$Travel_history_country[df_count$Travel_history_country == "United Kingdom"] <- "UK"

df_count$Country[df_count$Country == "United States"] <- "USA"
df_count$Travel_history_country[df_count$Travel_history_country == "United States"] <- "USA"

MPXV_cases_data_travel<-MPXV_cases_data
MPXV_cases_data_travel <- MPXV_cases_data_travel[!MPXV_cases_data_travel$ID %in% remove_from_travel_links, ]

df_count2 <- MPXV_cases_data_travel %>% filter(Travel_history_country!='')  %>% count(Travel_history_country)
names(df_count2)[names(df_count2) == "Travel_history_country"] <- "Travel_history_country"
names(df_count2)[names(df_count2) == "n"] <- "Travel_Count"

df_count2 <- subset(df_count2, !is.na(Travel_history_country))
df_count2


df_count2 <- df_count2 %>%
  left_join(MPXV_cases_data_travel, by = c("Travel_history_country" = "Travel_history_country"))

df_count2$Travel_history_country[df_count2$Travel_history_country == "United Kingdom"] <- "UK"
df_count2$Travel_history_country[df_count2$Travel_history_country == "United Kingdom"] <- "UK"

df_count2$Travel_history_country[df_count2$Travel_history_country == "United States"] <- "USA"
df_count2$Travel_history_country[df_count2$Travel_history_country == "United States"] <- "USA"





world1 <- map_data("world")



df_count2<-df_count2 %>% dplyr::select('Travel_history_country_lat','Travel_history_country_long','Travel_history_country',"Travel_Count")

world5<- world1 %>% 
  left_join(df_count2, by = c("region" = "Travel_history_country"))


world1<- world1 %>% 
  left_join(df_count, by = c("region" = "Country"))



world4<- subset(subset(subset(world1,!is.na(Travel_history_country)),Travel_history_country!='Western Europe'), !is.na(Travel_history_entry))

world4<-subset(world4,Travel_history_country_lat!='numeric(0)')
world4$Travel_history_country_lat <- unlist(world4$Travel_history_country_lat)
world4$Travel_history_country_long <- unlist(world4$Travel_history_country_long)


world4$date_travel <- as.Date(world4$Travel_history_entry)

world4$days_travel <- as.Date(cut(world4$date_travel, breaks = "day", start.on.monday = FALSE))
world4$days_travel <- format(world4$days_travel,"%d %B %Y")

world4$date1_travel <- as.Date(cut(world4$date_travel, breaks = "1 weeks", start.on.monday = FALSE))
world4$date2_travel <- as.Date(cut(world4$date_travel, breaks = "2 weeks", start.on.monday = FALSE))
world4$date3_travel <- as.Date(cut(world4$date_travel, breaks = "1 month", start.on.monday = FALSE))

world4 <- world4[!world4$ID %in% remove_from_travel_links, ]

world1$count_range <- cut(as.numeric(world1$Count), breaks = c(0, 10, 50, 100, Inf), right = TRUE, labels = c("<10", "10-50", "50-100", ">100"))


world5 <- world5 %>% dplyr::select("Travel_history_country_lat", "Travel_history_country_long", "region", "Travel_Count")
world5 <- unique(subset(world5, !is.na(Travel_Count)))
#world5 <- world5[!world5$ID %in% remove_from_travel_links, ]


cols <- c("#9ECAE1", "#3182BD", "#FC9272", "#DE2D26")
# show_col(cols)

mpxv_map <- ggplot() +
  theme_void() +
  coord_fixed() +
  geom_map(data = world1, map = world1, aes(long, lat, map_id = region), color = "white", fill = "grey90", size = 0.1) +
  geom_map(data = subset(world1, !is.na(count_range)), map = subset(world1, !is.na(count_range)), aes(long, lat, map_id = region, fill = count_range), color = "white", size = 0.1) +
  scale_fill_manual(values = cols, name = "Cases") +
  scale_color_manual(
    values = c("mediumseagreen", "#8c510a", "#d8b365", "antiquewhite3","tan", "grey70", "grey50", "grey20"),
    na.value = "grey90", name = "Travel History",
    labels = c("01 January 2050" = "Unknown Date")
    
     ) +
  scale_size_area(breaks = c(1, 10), labels = c("1", "10"), name = "Travel History Cases") +
  geom_point(
    data = world5,
    aes(
      x = as.double(unlist(Travel_history_country_long)),
      y = as.double(unlist(Travel_history_country_lat)), size = Travel_Count
    ),
    shape = 21, color = "black"
  ) +
  geom_curve(
    data = world4,
    aes(
      x = as.double(unlist(Travel_history_country_long)),
      y = as.double(unlist(Travel_history_country_lat)),
      xend = as.double(Country_long),
      yend = as.double(Country_lat), colour = as.factor(days_travel)
    ),
    size = 0.3
  ) +
  theme(legend.position = c(0.1, 0.4)) +
  theme(legend.direction = "vertical") +
  theme(legend.title = element_text(size = 8)) +
  guides(colour = guide_legend(override.aes = list(size = 3))) +
  scale_y_continuous(limits = c(-55, 90)) +
  scale_x_continuous(limits = c(-170, 170))

df_count <- subset(MPXV_cases_data, !is.na(days)) %>%
  dplyr::select("days", "Country") %>%
  arrange(days) %>%
  unique() %>%
  mutate(count = cumsum(!duplicated(Country))) %>%
  group_by(days) %>%
  mutate(count2 = max(count))
df_count

mpxv_cases_countries <- ggplot() +
  theme_bw() +
  stat_bin(data = MPXV_cases_data, aes(x = days, y = cumsum(..count..), fill = "Cases"), geom = "bar", color = "grey70", size = 0.5) +
  geom_line(data = df_count, aes(x = days, y = count2 * 30, color = "Countries"), size = 1) +
  geom_point(data = df_count, aes(x = days, y = count2 * 30, color = "Countries"), size = 2) +
  scale_fill_manual(values = c("#3182BD"), name = "") +
  scale_color_manual(values = c("red3"), name = "") +
  theme(legend.position = c(0.2, 0.8)) +
  theme(legend.title = element_blank()) +
  theme(legend.background = element_blank()) +
  xlab("") +
  ylab("Cummulative Cases") +
  scale_y_continuous(
    name = "Cummulative Cases",
    sec.axis = sec_axis(~ . / 30, name = "Countries")
  )

p <- plot_grid(mpxv_map, mpxv_cases_countries, ncol = 2, labels = c("A", "B"), rel_widths = c(0.75, 0.25))

ggsave(plot = p, "build/figures/travel-history.png", width = 18, height = 5, limitsize = FALSE)
