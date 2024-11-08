library(tidyverse)
source("verbs.R")
library(bibliometrix)
library(tosr)
library(igraph)
library(tidygraph)
library(lubridate)
# library(sjrdata)
library(openxlsx)
library(zoo)
library(journalabbr)
library(ggraph)
library(openxlsx)
library(XML)
library(plyr)

wos_scopus <-
  tosr::tosr_load("insuline.bib",
                  "insuline.txt"
                  # "insuline_2.txt"
                  # "insuline_1_3.txt",
                  # "insuline_1_4.txt"
                  )

tree_of_science <-
  tosr::tosR("insuline.bib",
             "insuline.txt"
             # "insuline_2.txt"
             # "insuline_1_3.txt",
             # "insuline_1_4.txt"
             )

wos <-
  bibliometrix::convert2df(c("insuline.txt"
                             # "insuline_2.txt"
                             # "insuline_1_3.txt",
                             # "insuline_1_4.txt"
                             )) |>  # create dataframe from wos file
  bibliometrix::metaTagExtraction(Field = "AU_CO" ) # Adding Country author's affiliation

scopus <-
  bibliometrix::convert2df(c("insuline.bib"
                             # "insuline_1b.bib"
                             ), # Create dataframe from scopus file
                           dbsource = "scopus",
                           format = "bibtex") |> 
  bibliometrix::metaTagExtraction(Field = "AU_CO" )  # Adding Country author's affiliation


CR_links <-
  get_references(wos_scopus$df)

write_csv(CR_links, "CR_links_insuline_1.csv")
# 
# CR_links <-
#   read_csv("CR_links_insuline_1.csv")

# country_df <-
#   get_country()

SO_links <-
  get_journals(wos_scopus$df, CR_links)

write_csv(SO_links, "SO_links_insuline_1.csv")
# 
# SO_links <-
#   read_csv("SO_links_insuline_1.csv")

AU_links <-
  get_authors(wos_scopus$df, CR_links)

write_csv(AU_links, "AU_links_insuline_1.csv")
# 
# AU_links <-
#   read_csv("AU_links_insuline_1.csv")

#### Figure 1 ####

wos_anual_production <-
  wos |>
  dplyr::select(PY) |>
  dplyr::count(PY, sort = TRUE) |>
  na.omit() |>
  dplyr::filter(PY >= 2000,
                PY < year(today())) |>
  dplyr::mutate(ref_type = "wos")

scopus_anual_production  <-
  scopus |>
  dplyr::select(PY) |>
  dplyr::count(PY, sort = TRUE) |>
  na.omit() |>
  dplyr::filter(PY >= 2000,
                PY < year(today())) |>
  dplyr::mutate(ref_type = "scopus")

total_anual_production <-
  wos_scopus$df |>
  dplyr::select(PY) |>
  dplyr::count(PY, sort = TRUE) |>
  na.omit() |>
  dplyr::filter(PY >= 2000,
                PY < year(today())) |>
  dplyr::mutate(ref_type = "total") |>
  dplyr::arrange(desc(PY))

wos_scopus_total_annual_production <-
  wos_anual_production |>
  bind_rows(scopus_anual_production,
            total_anual_production)

# Checking results of total
wos_scopus_total_annual_production_dummy <-
  total_anual_production |>
  dplyr::rename(n_total = n,
                ref_type_total = ref_type) |>
  left_join(wos_anual_production |>
              dplyr::rename(n_wos = n,
                            ref_type_wos = ref_type) ) |>
  left_join(scopus_anual_production |>
              dplyr::rename(n_scopus = n,
                            ref_type_scopus = ref_type)) |>
  # mutate(total = if_else(n_total < n_wos | n_total < n_scopus,
  #                        max(tibble(n_wos = n_wos, n_scopus = n_scopus)), # it could be improved
  #                        n_total)) |>
  replace_na(list(n_wos = 0,
                  n_scopus = 0,
                  n_total = 0,
                  ref_type_wos = "wos",
                  ref_type_scopus = "scopus"))
# select(-total)

wos_scopus_total_annual_production_total <-
  wos_scopus_total_annual_production_dummy |>
  select(PY,
         n = n_total,
         ref_type = ref_type_total)

wos_scopus_total_annual_production_scopus <-
  wos_scopus_total_annual_production_dummy |>
  select(PY,
         n = n_scopus,
         ref_type = ref_type_scopus)

wos_scopus_total_annual_production_wos <-
  wos_scopus_total_annual_production_dummy |>
  select(PY,
         n = n_wos,
         ref_type = ref_type_wos)

wos_scopus_total_annual_production <-
  wos_scopus_total_annual_production_total |>
  bind_rows(wos_scopus_total_annual_production_scopus,
            wos_scopus_total_annual_production_wos)

figure_1_data <-
  wos_scopus_total_annual_production |>
  mutate(PY = replace_na(PY, replace = 0)) |>
  pivot_wider(names_from = ref_type,
              values_from = n) |>
  arrange(desc(PY))

range_annual <-
  tibble(PY = 2000:2022)

TC_wos <-
  wos |>
  dplyr::select(PY, TC) |>
  dplyr::group_by(PY) |>
  dplyr::summarise(TC_sum = sum(TC)) |>
  arrange(desc(PY)) |>
  na.omit() %>%
  dplyr::right_join(range_annual,
                    by = "PY") %>%
  tidyr::replace_na(list(TC_sum = 0))


TC_scopus <-
  scopus |>
  dplyr::select(PY, TC) |>
  dplyr::group_by(PY) |>
  dplyr::summarise(TC_sum = sum(TC)) |>
  arrange(desc(PY)) |>
  na.omit() %>%
  dplyr::right_join(range_annual,
                    by = "PY") %>%
  tidyr::replace_na(list(TC_sum = 0))

TC_all <-
  TC_scopus |>
  left_join(TC_wos,
            by = "PY",
            suffix = c("_wos",
                       "_scopus")) |>
  replace_na(replace = list(TC_sum_scopus = 0)) |>
  mutate(TC_sum_all = TC_sum_wos + TC_sum_scopus,
         TC_total = sum(TC_sum_all),
         TC_percentage = round(TC_sum_all/TC_total, digits = 2)) |>
  select(PY, TC_sum_all, TC_percentage) |>
  filter(PY <= 2022) |>
  filter(PY >= 2000) |>
  arrange(desc(PY))

#### Table 2 - Countries ####

wos_scopus_countries <-
  wos_scopus$df |>
  select(SR, AU_CO, TC) |>
  separate_rows(AU_CO, sep = ";") |>
  unique() |>
  drop_na()

wos_scopus_countries_journals <-
  wos_scopus_countries |>
  left_join(wos_scopus$df |>
              select(SR, SO, PY),
            by = "SR")

# scimago_2020 <-
#   read_csv2("scimago2020.csv", show_col_types = FALSE) |>
#   select(SO = Title,
#          quartile = "SJR Best Quartile") |>
#   mutate(PY = 2020)

scimago <-
  #read_csv("https://docs.google.com/spreadsheets/d/1K_3QqjcD8Hab2ehXwE_1cm-6yhSoet13Q7qsULlnN1Y/export?format=csv&gid=1875866918") |>
  read_csv("scimago_2024_combined.csv") |>
  # select(-1) |>
  select(PY = PY,
         SO = journal,
         quartile = categoria) |>
  mutate(SO = str_to_upper(SO)) |>
  unique()

# scimago_2021 <-
#   read_csv2("scimago2020.csv", show_col_types = FALSE) |>
#   select(SO = Title,
#          quartile = "SJR Best Quartile") |>
#   mutate(PY = 2021)

# scimago_2020_2021 <-
#   scimago_2020 |>
#   bind_rows(scimago_2021) |>
#   select(PY, SO, quartile)

# scimago_1 <-
#   sjr_journals |>
#   select(PY = year,
#          SO = title,
#          quartile = sjr_best_quartile) |>
#   mutate(SO = str_to_upper(SO),
#          PY = as.numeric(PY)) |>
#   bind_rows(scimago_2020_2021)

au_co_quartile <-
  wos_scopus_countries_journals |>
  left_join(scimago, by = c("PY", "SO")) |>
  group_by(AU_CO) |>
  filter(!duplicated(SR))

wos_scopus_quartile <-
  wos_scopus$df |>
  left_join(scimago, by = c("PY", "SO")) |>
  group_by(AU_CO) |>
  filter(!duplicated(SR))

wos_scopus_countries_journals_scimago <-
  wos_scopus_countries_journals |>
  left_join(scimago, by = c("PY", "SO")) |>
  group_by(AU_CO) |>
  filter(!duplicated(SR)) |>
  # drop_na() |>
  # filter(n() != 1) |>
  select(SO, AU_CO, TC, quartile)
# filter(quartile != "-")

# df_dummy <-
#  wos_scopus_tos$df |>
#  left_join(scimago, by = c("PY", "SO")) |>
#  filter(!duplicated(SR))

table_2a <-
  wos_scopus_countries |>
  dplyr::select(AU_CO) |>
  dplyr::group_by(AU_CO) |>
  dplyr::summarise(count_co = n()) |>
  dplyr::mutate(percentage_co = count_co / sum(count_co) * 100,
                percentage_co = round(percentage_co, digits = 2)) |>
  dplyr::arrange(desc(count_co))

table_2b <-
  wos_scopus_countries_journals_scimago |>
  dplyr::select(AU_CO, TC) |>
  dplyr::group_by(AU_CO) |>
  dplyr::summarise(citation = sum(TC)) |>
  dplyr::mutate(percentage_ci = citation / sum(citation) * 100) |>
  dplyr::arrange(desc(citation))

table_2c <-
  wos_scopus_countries_journals_scimago |>
  dplyr::select(AU_CO, quartile) |>
  dplyr::group_by(AU_CO) |>
  dplyr::count(quartile, sort = TRUE) |>
  pivot_wider(names_from = quartile,
              values_from = n) |>
  dplyr::select(AU_CO, Q1, Q2, Q3, Q4) |>
  dplyr::mutate(Q1 = replace_na(Q1, 0),
                Q2 = replace_na(Q2, 0),
                Q3 = replace_na(Q3, 0),
                Q4 = replace_na(Q4, 0))

table_2 <-
  table_2a |>
  left_join(table_2b, by = "AU_CO") |>
  left_join(table_2c, by = "AU_CO") |>
  mutate(percentage_ci = round(percentage_ci, digits = 2),
         no_category = count_co - (Q1 + Q2 + Q3 + Q4)) |>
  slice(1:10)

#### Figure 2 - Countries ####

df_dummy_1 <-
  wos |>
  select(SR, AU_CO) |>
  separate_rows(AU_CO, sep = ";") |>
  unique()

df_dummy_2 <-
  scopus |>
  select(SR, AU_CO) |>
  separate_rows(AU_CO, sep = ";")

df_dummy_3 <-
  df_dummy_1 |>
  bind_rows(df_dummy_2) |>
  unique() |>
  na.omit()

edgelist_wos_countries <- data.frame(from = character(),
                                     to = character(),
                                     SR = character(),
                                     # year = as.numeric(),
                                     stringsAsFactors = FALSE
)
# table_ids <- table(author_1$doi)
# table_ids_0 <- data.frame(table_ids)
# table_ids_1 <- table_ids_0[table_ids_0$Freq >= 2,]
list_ids_1 <-
  df_dummy_1 |>
  select(SR) |>
  group_by(SR) |>
  filter(n() > 1) |>
  unique()

for (i in list_ids_1$SR) {
  df_1 = df_dummy_1[df_dummy_1$SR == i,]
  df_2 = combn(df_1$AU_CO, 2, simplify = FALSE)
  df_3 = data.frame((t(data.frame(df_2))), i)
  colnames(df_3) = c("from", "to", "SR")
  # df_4 <- df_3 |> bind_cols(df_1 |> select(year) |> unique())
  edgelist_wos_countries = rbind(edgelist_wos_countries, df_3)
}

edgelist_wos_countries_weighted <-
  edgelist_wos_countries |>
  dplyr::select(from, to) |>
  dplyr::group_by(from, to) |>
  dplyr::count(from, to) |>
  filter(from != to)

edgelist_scopus_countries <- data.frame(from = character(),
                                        to = character(),
                                        SR = character(),
                                        # year = as.numeric(),
                                        stringsAsFactors = FALSE
)
# table_ids <- table(author_1$doi)
# table_ids_0 <- data.frame(table_ids)
# table_ids_1 <- table_ids_0[table_ids_0$Freq >= 2,]
list_ids_2 <-
  df_dummy_2 |>
  select(SR) |>
  group_by(SR) |>
  filter(n() > 1) |>
  unique()

for (i in list_ids_2$SR) {
  df_1 = df_dummy_2[df_dummy_2$SR == i,]
  df_2 = combn(df_1$AU_CO, 2, simplify = FALSE)
  df_3 = data.frame((t(data.frame(df_2))), i)
  colnames(df_3) = c("from", "to", "SR")
  # df_4 <- df_3 |> bind_cols(df_1 |> select(year) |> unique())
  edgelist_scopus_countries = rbind(edgelist_scopus_countries, df_3)
}

edgelist_scopus_countries_weighted <-
  edgelist_scopus_countries |>
  dplyr::select(from, to) |>
  dplyr::group_by(from, to) |>
  dplyr::count(from, to) |>
  filter(from != to)

# Merging both datasets

edgelist_wos_scopus_countries <-
  edgelist_wos_countries |>
  bind_rows(edgelist_scopus_countries) |>
  unique() |>
  mutate(PY = str_extract(SR, "[0-9]{4}"))

edgelist_wos_scopus_countries_weighted <-
  edgelist_wos_scopus_countries |>
  dplyr::select(from, to) |>
  dplyr::group_by(from, to) |>
  dplyr::count(from, to) |>
  filter(from != to) %>%
  dplyr::rename(Weight = n)

edgelist_wos_scopus_countries_weighted_properties <-
  edgelist_wos_scopus_countries_weighted |>
  tidygraph::as_tbl_graph(directed = FALSE) |>
  tidygraph::activate(edges) %>%
  # tidygraph::rename(Weight = n) %>%
  activate(nodes) |>
  dplyr::mutate(community = tidygraph::group_louvain(),
                degree = tidygraph::centrality_degree(),
                community = as.factor(community))

# net_dummy_wos <-
#   df_dummy_1 |>
#   group_by(SR) |>
#   filter(n() > 1)
#   tidyr::expand_grid(from = df_dummy_1$SR,
#                      to = df_dummy_1$AU_CO) |>
#   unique() |>
#   filter( from != to)
#
#
# net_dummy_wos_1 <-
#   wos |>
#   biblioNetwork(analysis = "coupling",
#                 network = "countries") |>
#   graph_from_adjacency_matrix(mode = "undirected",
#                               weighted = TRUE) |>
#   simplify() |>
#   as_tbl_graph() |>
#   activate(nodes) |>
#   mutate(communities = group_components(type = "weak")) |>
#   filter(communities == 1)

#### Table 3 - Journals ####

table_1 <-
  tibble(wos = length(wos$SR), # Create a dataframe with the values.
         scopus = length(scopus$SR),
         total = length(wos_scopus$df$SR))

wos_journal <-
  wos |>
  dplyr::select(journal = SO) |>
  na.omit() |>
  dplyr::group_by(journal) |>
  dplyr::count(journal, sort = TRUE) |>
  dplyr::slice(1:20) |>
  dplyr::rename(publications = n) |>
  dplyr::mutate(database = "wos")

scopus_journal <-
  scopus |>
  dplyr::select(journal = SO) |>
  na.omit() |>
  dplyr::count(journal, sort = TRUE) |>
  dplyr::slice(1:20) |>
  dplyr::rename(publications = n) |>
  dplyr::mutate(database = "scopus")

total_journal <-
  wos_scopus$df |>
  dplyr::select(journal = SO) |>
  na.omit() |>
  dplyr::count(journal, sort = TRUE) |>
  dplyr::slice(1:20) |>
  dplyr::rename(publications = n) |>
  dplyr::mutate(database = "total")

wos_scopus_total_journal <-
  wos_journal |>
  dplyr::bind_rows(scopus_journal,
                   total_journal) |>
  pivot_wider(names_from = database,
              values_from = publications) |>
  dplyr::arrange(desc(total)) |>
  dplyr::slice(1:10) |>
  dplyr::mutate_all(~replace_na(., 0)) |>
  dplyr::mutate(percentage = total / table_1 |>
                  dplyr::pull(total),
                percentage = round(percentage, digits = 2))

# Data for visualización of Journal network

journal_citation_graph_weighted_tbl_small <-
  SO_links |>
  dplyr::select(JI_main, JI_ref) |>
  dplyr::group_by(JI_main, JI_ref) |>
  dplyr::count() |>
  dplyr::rename(weight = n) |>
  as_tbl_graph(directed = FALSE) |>
  # convert(to_simple) |>
  activate(nodes) |>
  dplyr::mutate(components = tidygraph::group_components(type = "weak"))  |>
  dplyr::filter(components == 1) |>
  activate(nodes) |>
  dplyr::mutate(degree = centrality_degree(),
                community = tidygraph::group_louvain()) |>
  dplyr::select(-components) |>
  dplyr::filter(degree >= 1)
# activate(edges) |>
# dplyr::filter(weight != 1)

communities <-
  journal_citation_graph_weighted_tbl_small |>
  activate(nodes) |>
  data.frame() |>
  dplyr::count(community, sort = TRUE) |>
  dplyr::slice(1:10) |>
  dplyr::select(community) |>
  dplyr::pull()
# Filtering biggest communities
journal_citation_graph_weighted_tbl_small_fig <-
  journal_citation_graph_weighted_tbl_small |>
  activate(nodes) |>
  dplyr::filter(community %in% communities)

# Selecting nodes to show

jc_com_1 <-
  journal_citation_graph_weighted_tbl_small_fig |>
  activate(nodes) |>
  dplyr::filter(community == communities[1]) |>
  dplyr::mutate(degree = centrality_degree()) |>
  dplyr::arrange(desc(degree)) |>
  dplyr::slice(1:10) |>
  data.frame() |>
  dplyr::select(name)
jc_com_2 <-
  journal_citation_graph_weighted_tbl_small_fig |>
  activate(nodes) |>
  dplyr::filter(community == communities[2]) |>
  dplyr::mutate(degree = centrality_degree()) |>
  dplyr::arrange(desc(degree)) |>
  dplyr::slice(1:10) |>
  data.frame() |>
  dplyr::select(name)
jc_com_3 <-
  journal_citation_graph_weighted_tbl_small_fig |>
  activate(nodes) |>
  dplyr::filter(community == communities[3]) |>
  dplyr::mutate(degree = centrality_degree()) |>
  dplyr::arrange(desc(degree)) |>
  dplyr::slice(1:10) |>
  data.frame() |>
  dplyr::select(name)
jc_com_4 <-
  journal_citation_graph_weighted_tbl_small_fig |>
  activate(nodes) |>
  dplyr::filter(community == communities[4]) |>
  dplyr::mutate(degree = centrality_degree()) |>
  dplyr::arrange(desc(degree)) |>
  dplyr::slice(1:10) |>
  data.frame() |>
  dplyr::select(name)
jc_com_5 <-
  journal_citation_graph_weighted_tbl_small_fig |>
  activate(nodes) |>
  dplyr::filter(community == communities[5]) |>
  dplyr::mutate(degree = centrality_degree()) |>
  dplyr::arrange(desc(degree)) |>
  dplyr::slice(1:10) |>
  data.frame() |>
  dplyr::select(name)
jc_com_6 <-
  journal_citation_graph_weighted_tbl_small_fig |>
  activate(nodes) |>
  dplyr::filter(community == communities[6]) |>
  dplyr::mutate(degree = centrality_degree()) |>
  dplyr::arrange(desc(degree)) |>
  dplyr::slice(1:10) |>
  data.frame() |>
  dplyr::select(name)
jc_com_7<-
  journal_citation_graph_weighted_tbl_small_fig |>
  activate(nodes) |>
  dplyr::filter(community == communities[7]) |>
  dplyr::mutate(degree = centrality_degree()) |>
  dplyr::arrange(desc(degree)) |>
  dplyr::slice(1:10) |>
  data.frame() |>
  dplyr::select(name)
jc_com_8 <-
  journal_citation_graph_weighted_tbl_small_fig |>
  activate(nodes) |>
  dplyr::filter(community == communities[8]) |>
  dplyr::mutate(degree = centrality_degree()) |>
  dplyr::arrange(desc(degree)) |>
  dplyr::slice(1:10) |>
  data.frame() |>
  dplyr::select(name)
jc_com_9 <-
  journal_citation_graph_weighted_tbl_small_fig |>
  activate(nodes) |>
  dplyr::filter(community == communities[9]) |>
  dplyr::mutate(degree = centrality_degree()) |>
  dplyr::arrange(desc(degree)) |>
  dplyr::slice(1:10) |>
  data.frame() |>
  dplyr::select(name)
jc_com_10 <-
  journal_citation_graph_weighted_tbl_small_fig |>
  activate(nodes) |>
  dplyr::filter(community == communities[10]) |>
  dplyr::mutate(degree = centrality_degree()) |>
  dplyr::arrange(desc(degree)) |>
  dplyr::slice(1:10) |>
  data.frame() |>
  dplyr::select(name)
jc_com <-
  jc_com_1 |>
  bind_rows(jc_com_2,
            jc_com_3,
            # jc_com_4,
            # jc_com_5,
            # jc_com_6,
            # jc_com_7,
            # jc_com_8,
            # jc_com_9,
            # jc_com_10
  )

SO_graph <-
  journal_citation_graph_weighted_tbl_small_fig |>
  activate(nodes) |>
  dplyr::filter(name %in% jc_com$name) |>
  dplyr::mutate(degree = centrality_degree(),
                community = factor(community)) |>
  dplyr::filter(degree != 0)

SO_graph_edges <-
  SO_graph %>%
  tidygraph::as_tbl_graph() %>%
  tidygraph::activate(edges) %>%
  tidygraph::as_tibble() %>%
  dplyr::rename(Source = "from",
                Target = "to")

SO_graph_nodes <-
  SO_graph %>%
  tidygraph::as_tbl_graph() %>%
  tidygraph::activate(nodes) %>%
  tidygraph::as_tibble() %>%
  dplyr::rename(author = name) |>
  tibble::rownames_to_column("name") %>%
  dplyr::rename(id = "name",
                Label = "author")


#### Table 4 - Authors ####

data_biblio_wos <- biblioAnalysis(wos)

wos_authors <-
  data_biblio_wos$Authors |>
  data.frame() |>
  dplyr::rename(authors_wos = AU, papers_wos = Freq) |>
  dplyr::arrange(desc(papers_wos)) |>
  dplyr::slice(1:20) |>
  dplyr::mutate(database_wos = "wos")

data_biblio_scopus <- biblioAnalysis(scopus)

scopus_authors <-
  data_biblio_scopus$Authors |>
  data.frame() |>
  dplyr::rename(authors_scopus = AU, papers_scopus = Freq) |>
  dplyr::arrange(desc(papers_scopus)) |>
  dplyr::slice(1:20) |>
  dplyr::mutate(database_scopus = "scopus")

data_biblio_total <- biblioAnalysis(wos_scopus$df)

total_authors <-
  data_biblio_total$Authors |>
  data.frame() |>
  dplyr::rename(authors_total = AU,
                papers_total = Freq) |>
  dplyr::arrange(desc(papers_total)) |>
  dplyr::slice(1:20) |>
  dplyr::mutate(database_total = "total")

wos_scopus_authors <-
  wos_authors |>
  dplyr::bind_cols(scopus_authors,
                   total_authors) %>%
  dplyr::filter(authors_wos != "NA NA") %>%
  dplyr::filter(authors_wos != "NA N") %>%
  dplyr::filter(authors_scopus != "NA NA") %>%
  dplyr::filter(authors_scopus != "NA N") %>%
  dplyr::filter(authors_total != "NA NA") %>%
  dplyr::filter(authors_total != "NA N")


### Choosing the ASN ####

AU_graph <-
  AU_links |>
  dplyr::select(-PY) |>
  dplyr::group_by(from, to) |>
  dplyr::count() |>
  dplyr::rename(weight = n) |>
  tidygraph::as_tbl_graph(directed = FALSE) |>
  activate(nodes) |>
  # dplyr::mutate(components = tidygraph::group_components(type = "weak")) |>
  # dplyr::filter(components == 1) |>
  dplyr::mutate(degree = centrality_degree(),
                community = as.factor(group_louvain()))

safe_convert <- function(i) {
  tryCatch({
    tidygraph::convert(
      AU_graph, 
      to_local_neighborhood,
      node = which(.N()$name == wos_scopus_authors$authors_total[i]),
      order = 1,
      mode = "all"
    )
  }, error = function(e) {
    message(paste("Error processing author", i, ":", e))
    NULL
  })
}


# Apply the function to each author
ego_1 <- safe_convert(1)
ego_2 <- safe_convert(2)
ego_3 <- safe_convert(3)
ego_4 <- safe_convert(4)
ego_5 <- safe_convert(5)
ego_6 <- safe_convert(6)
ego_7 <- safe_convert(7)
ego_8 <- safe_convert(8)
ego_9 <- safe_convert(9)
ego_10 <- safe_convert(10)


# ego_1 <-
#   AU_graph |>
#   tidygraph::convert(to_local_neighborhood,
#                      node = which(.N()$name == wos_scopus_authors$authors_total[1]),
#                      order = 1,
#                      mode = "all")
# 
# ego_2 <-
#   AU_graph |>
#   tidygraph::convert(to_local_neighborhood,
#                      node = which(.N()$name == wos_scopus_authors$authors_total[2]),
#                      order = 1,
#                      mode = "all")
# 
# ego_3 <-
#   AU_graph |>
#   tidygraph::convert(to_local_neighborhood,
#                      node = which(.N()$name == wos_scopus_authors$authors_total[3]),
#                      order = 1,
#                      mode = "all")
# 
# ego_4 <-
#   AU_graph |>
#   tidygraph::convert(to_local_neighborhood,
#                      node = which(.N()$name == wos_scopus_authors$authors_total[4]),
#                      order = 1,
#                      mode = "all")

# ego_5 <-
#   AU_graph |>
#   tidygraph::convert(to_local_neighborhood,
#                      node = which(.N()$name == wos_scopus_authors$authors_wos[5]),
#                      order = 1,
#                      mode = "all")

# ego_5 <-
#   AU_graph |>
#   tidygraph::convert(to_local_neighborhood,
#                      node = which(.N()$name == wos_scopus_authors$authors_total[5]),
#                      order = 1,
#                      mode = "all")
# 
# ego_6 <-
#   AU_graph |>
#   tidygraph::convert(to_local_neighborhood,
#                      node = which(.N()$name == wos_scopus_authors$authors_total[6]),
#                      order = 1,
#                      mode = "all")
# 
# ego_7 <-
#   AU_graph |>
#   tidygraph::convert(to_local_neighborhood,
#                      node = which(.N()$name == wos_scopus_authors$authors_total[7]),
#                      order = 1,
#                      mode = "all")
# 
# ego_8 <-
#   AU_graph |>
#   tidygraph::convert(to_local_neighborhood,
#                      node = which(.N()$name == wos_scopus_authors$authors_total[8]),
#                      order = 1,
#                      mode = "all")
# 
# ego_9 <-
#   AU_graph |>
#   tidygraph::convert(to_local_neighborhood,
#                      node = which(.N()$name == wos_scopus_authors$authors_total[9]),
#                      order = 1,
#                      mode = "all")
# 
# ego_10 <-
#   AU_graph |>
#   tidygraph::convert(to_local_neighborhood,
#                      node = which(.N()$name == wos_scopus_authors$authors_total[10]),
#                      order = 1,
#                      mode = "all")

# Create a list of ego graphs
ego_list <- list(ego_1, ego_2, ego_3, ego_4, ego_5, ego_6, ego_7, ego_8, ego_9, ego_10)

# Remove any NULL elements from the list
ego_list <- Filter(Negate(is.null), ego_list)

# Join the remaining graphs iteratively
AU_egos <- Reduce(function(x, y) tidygraph::graph_join(x, y), ego_list)

# Continue with your node processing
AU_egos <- AU_egos |>
  activate(nodes) |>
  dplyr::mutate(component = tidygraph::group_components(),
                component = factor(component),
                degree = tidygraph::centrality_degree())


# AU_egos <-
#   ego_1 |>
#   tidygraph::graph_join(ego_2) |>
#   tidygraph::graph_join(ego_3) |>
#   tidygraph::graph_join(ego_4) |>
#   tidygraph::graph_join(ego_5) |>
#   tidygraph::graph_join(ego_6) |>
#   tidygraph::graph_join(ego_7) |>
#   tidygraph::graph_join(ego_8) |>
#   tidygraph::graph_join(ego_9) |>
#   tidygraph::graph_join(ego_10) |>
#   activate(nodes) |>
#   dplyr::mutate(component = tidygraph::group_components(),
#                 component = factor(component),
#                 degree = tidygraph::centrality_degree())

AU_graph_edges <-
  AU_egos %>%
  tidygraph::as_tbl_graph() %>%
  tidygraph::activate(edges) %>%
  tidygraph::as_tibble() %>%
  dplyr::rename(Source = "from",
                Target = "to")

AU_graph_nodes <-
  AU_egos %>%
  tidygraph::as_tbl_graph() %>%
  tidygraph::activate(nodes) %>%
  tidygraph::as_tibble() %>%
  dplyr::rename(author = name) |>
  tibble::rownames_to_column("name") %>%
  dplyr::rename(id = "name",
                Label = "author")

####  Tree of Science ####

nodes <-  # Create a dataframe with the fullname of articles
  tibble(name = V(wos_scopus$graph)$name) |>
  left_join(wos_scopus$nodes,
            by = c("name" = "ID_TOS"))

wos_scopus_citation_network_1 <- # Add the article names to the citation network
  wos_scopus$graph |>
  igraph::set.vertex.attribute(name = "full_name",
                               index = V(wos_scopus$graph)$name,
                               value = nodes$CITE)

nodes_1 <- # Create a dataframe with subfields (clusters)
  tibble(name = V(wos_scopus_citation_network_1)$name,
         cluster = V(wos_scopus_citation_network_1)$subfield,
         full_name = V(wos_scopus_citation_network_1)$full_name)

nodes_2 <- # Count the number of articles per cluster
  nodes_1 |>
  dplyr::count(cluster, sort = TRUE) |>
  dplyr::mutate(cluster_1 = row_number()) |>
  dplyr::select(cluster, cluster_1)

nodes_3 <-
  nodes_1 |>
  left_join(nodes_2) |>
  dplyr::rename(subfield = cluster_1) |>
  dplyr::select(name, full_name, subfield)

edge_list <-
  get.edgelist(wos_scopus_citation_network_1) |>
  data.frame() |>
  dplyr::rename(Source = X1, Target = X2)

wos_scopus_citation_network <-
  graph.data.frame(d = edge_list,
                   directed = TRUE,
                   vertices = nodes_3)

nodes_full_data <-
  tibble(name = V(wos_scopus_citation_network)$name,
         cluster = V(wos_scopus_citation_network)$subfield,
         full_name = V(wos_scopus_citation_network)$full_name)

cluster_1 <-
  wos_scopus_citation_network |>
  delete.vertices(which(V(wos_scopus_citation_network)$subfield != 1))

cluster_1_page_rank <-
  cluster_1 |>
  set.vertex.attribute(name = "page_rank",
                       value = page_rank(cluster_1)$vector)

cluster_1_df <-
  tibble(name = V(cluster_1_page_rank)$name,
         full_name = V(cluster_1_page_rank)$full_name,
         page_rank = V(cluster_1_page_rank)$page_rank,
         cluster = V(cluster_1_page_rank)$subfield,)

cluster_2 <-
  wos_scopus_citation_network |>
  delete.vertices(which(V(wos_scopus_citation_network)$subfield != 2))

cluster_2_page_rank <-
  cluster_2 |>
  set.vertex.attribute(name = "page_rank",
                       value = page_rank(cluster_2)$vector)

cluster_2_df <-
  tibble(name = V(cluster_2_page_rank)$name,
         full_name = V(cluster_2_page_rank)$full_name,
         page_rank = V(cluster_2_page_rank)$page_rank,
         cluster = V(cluster_2_page_rank)$subfield,)

cluster_3 <-
  wos_scopus_citation_network |>
  delete.vertices(which(V(wos_scopus_citation_network)$subfield != 3))

cluster_3_page_rank <-
  cluster_3 |>
  set.vertex.attribute(name = "page_rank",
                       value = page_rank(cluster_3)$vector)

cluster_3_df <-
  tibble(name = V(cluster_3_page_rank)$name,
         full_name = V(cluster_3_page_rank)$full_name,
         page_rank = V(cluster_3_page_rank)$page_rank,
         cluster = V(cluster_3_page_rank)$subfield,)

ToS <-
  tree_of_science %>%
  dplyr::rename(full_name = "cite") %>%
  dplyr::mutate(page_rank = NA) %>%
  dplyr::filter(TOS != "Leaves") %>%
  dplyr::mutate(year = stringr::str_extract(full_name,
                                            "[0-9]{4}")) %>%
  dplyr::bind_rows(cluster_1_df %>%
                     select(TOS = cluster,
                            full_name,
                            page_rank) %>%
                     mutate(year = stringr::str_extract(full_name,
                                                        "[0-9]{4}"),
                            TOS = "Branch 1")) %>%
  dplyr::bind_rows(cluster_2_df %>%
                     select(TOS = cluster,
                            full_name,
                            page_rank) %>%
                     mutate(year = stringr::str_extract(full_name,
                                                        "[0-9]{4}"),
                            TOS = "Branch 2")) %>%
  dplyr::bind_rows(cluster_3_df %>%
                     select(TOS = cluster,
                            full_name,
                            page_rank) %>%
                     mutate(year = stringr::str_extract(full_name,
                                                        "[0-9]{4}"),
                            TOS = "Branch 3")) %>%
  dplyr::select(-page_rank)

#### Citation Network ####

wos_scopus_citation_network_clusters <-
  wos_scopus_citation_network |>
  delete.vertices(which(V(wos_scopus_citation_network)$subfield != 1 & # filter clusters
                          V(wos_scopus_citation_network)$subfield != 2 &
                          V(wos_scopus_citation_network)$subfield != 3))

edges_citation_network <-
  wos_scopus_citation_network_clusters %>%
  tidygraph::as_tbl_graph() %>%
  tidygraph::activate(edges) %>%
  tidygraph::as_tibble() %>%
  dplyr::rename(Source = "from",
                Target = "to")

nodes_citation_network <-
  wos_scopus_citation_network_clusters %>%
  tidygraph::as_tbl_graph() %>%
  tidygraph::activate(nodes) %>%
  tidygraph::as_tibble() %>%
  dplyr::rename(author = name) |>
  tibble::rownames_to_column("name") %>%
  dplyr::rename(id = "name",
                Label = "full_name")

#### Exporting data ####

list_of_files <- list(wos_scopus = wos_scopus$df,
                      wos = wos,
                      scopus = scopus,
                      reference_df = CR_links,
                      journal_df  = SO_links,
                      author_df = AU_links,
                      TC_all = TC_all,
                      figure_1_data = figure_1_data,
                      au_co_quartile = au_co_quartile,
                      table_2_country = table_2,
                      figure_2_country_wos_scopus = edgelist_wos_scopus_countries,
                      figure_2_country_wos_scopus_1 = edgelist_wos_scopus_countries_weighted,
                      table_3_journal = wos_scopus_total_journal,
                      table_4_authors = wos_scopus_authors,
                      AU_links = AU_links,
                      treeofscience = ToS,
                      ToS_edges = edges_citation_network,
                      TOS_nodes = nodes_citation_network,
                      SO_edges = SO_graph_edges,
                      SO_nodes = SO_graph_nodes,
                      AU_ego_edges = AU_graph_edges,
                      AU_ego_nodes = AU_graph_nodes

)


write.xlsx(list_of_files, file = "all_data_insuline_1.xlsx")

