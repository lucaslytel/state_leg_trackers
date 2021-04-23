#Tracker Updating Pull
library(googlesheets)
library(googlesheets4)
library(RPostgreSQL)
library(RPostgres)
library(tidyverse)
library(sqldf)
library(dplyr)
library(scales)

#Function
update_gs <- function(df) {
    while(i <= nrow(df)) {
        #subset the data
        data <- raw_data[raw_data$state == as.character(df[i,1]),]
        data$state <- NULL
        rownames(data) <- NULL
        #get the key
        G_key <- gs_key(as.character(df[i,2]))
        if(df[i,1] == "NE" | df[i,1] == "DC") {
            gs_edit_cells(G_key, ws = 3, input = data, anchor = "A2", col_names = FALSE)
        }
        else {
            gs_edit_cells(G_key, ws = 4, input = data, anchor = "A2", col_names = FALSE)
        }
        i <- i + 1
    }
}
#Create Tibble
states <-   c("NH","NY","NJ","ME","OH","DE","MA","MD","PA","RI","VA","VT","IA","NE","ND",
              "WI","MI","IN","MO","SC","FL","KY","GA","TX","AL","MS","NC","LA","TN","OK",
              "WV","AR","ID","WY","CO","UT","AZ","NM","MT","CA","AK","WA","OR","NV","HI", "DC")

keys <- c("1z-h56KxT-DQziqazKgcnR_BszzH3L9LtccHKdBfgXCQ","1rukXCk1PG7Ypww_tWIvkiH_OwciQbY7bly7uAeNOBMA",
          "1L2fr8Valcd2PKwvFx52zqQCJjy-1KaG2eAYriKJXGHA","1LnqNJewhAPG3qePnRHfv23wuuGjzHmhyqbtj9lPvpDI",
          "1X1iqRzdCXOTO1Zdpbco65cUBcaqh4EXvr8jF9G1oo2U","1EB2QKmCW94JDYKozZV3p0AIeGcOCz679FENM0cgtO0s",
          "1mDZdmOWW31h8PWqkBpGmQ8ls8VtNVXdSmPrFPL3gOX8","1LSfmiovdMb7Kbo-y65T7TLjIIQeBCmNtznOVzFXVEUM",
          "1lMnPnXQ2g1wxAzsu1NOGtLYy_gjzcEvrl7zEq61c23o","1na5e_c6wCO1H_HoHAQM4REEbR1w0lNROsmEtuKL1vKM",
          "17xRMkflBkQNukUbAL9mMtVgOKpe5GSNb2Oa7ZFV_zhs","1kGdSk_mMewEwazS9ALlxaJHv1_X7R-nA-ldJa9cBnAw",
          "1oFwxojfgPw5Z4KBqh8jn0O7c0XFo1zf_JCNvOb4Pv7M","1KmYPuocvKBXVuKyib-sPdwdjM6XftOBsgSZ-O6Ja9v4",
          "1J2n_DP8eCEUE-tviCXxKNEGn_ITobCBERuXpoiDYhg4","1EVoBP3AyLAgPjBDNMPiQqhNEOfDeNFD7douNXrv6_Cg",
          "1c9FdmGdqAeAP3CkW4yomzoSCt5T_2QZZRYFRnxRMCEc","1tD83AfvYPo3mvftCwvo45w0ZuPRxv4RA-2tR6DDQjc4",
          "1XEPLkblrfqFOZDbltnetxxhnp02CIrx5QSNPfajK1GE","1mbx87bFv5APzH1RzPhSvtX8DS1Q7ieQeCvdRqpvWrWs",
          "1yDj558f6VyD5-VbZ8wtljN9npXWdpQoPpRoonBH76Ig","18W3EU2Iys_yCj5i6tXx2thmMiDgMRSN3FbfJ6ZIv5Mc",
          "1kxxxPsrNMVdPZ9parMxilhjFdoVrR6HjBHufOV5aVqo","1kgEJKQ9UTMyMZRlU8fiTaPWAbkIZ3XlQUSykTTTStt8",
          "1GqYLBD31STr_s4UrI4c_UgE_kcMlScK-XZwYsFbJjaE","1Mgp1PPewK8SkucjwvDBBDPxaBHrp-vBjswnR0nhdVBs",
          "1O0UUg8sdGSWSynXh69owBtJVQsSgKY2p2CzTSjGWylc","1eScEaR61GloZTOrU8TkD07FccD3JU0g-98dLszwYTGs",
          "1-Dpuxn8zw0AcqHDSX8rC38ieRc-sF2dn79v_hJFZFJA","1d18QH_Ize4slT_JfLN8DaaN4TwDKp8zS6p1jzvAP318",
          "1CDqvltqnGu3xWgUIMVERDF5o5rVlwGpFQfoYuzK2wj8","1Xtjiox9cIoAWMBR0fX-4BcT-MPBW7O8Fpu9LEACLrfo",
          "1XTIO-J7HhaO4RQZHnImMyNwtxb6X4gt8MVrCSKLBNdw","1RfWdPtn3sa-qTDAXr43tTtywedvJmZQ6u-gp2cm3go4",
          "1W7kKdRMR883-4mfWqHsYmO_AguAMh7LBkUCInG0IiLY","18s7zou6laxa6BwWGyGLXWltWM_bH4YktXJGuYy5bnqU",
          "1GwYMcNgvWSBs_H0W0rttz7p6YBJ9VobXOI3S6bsmXpU","1U3WBsaWbBxMYe3Mh3qtyU17MnMSIyUF-1G8NTIqJ8ew",
          "1FnK8nlDaj1kUgEg3_jFwUgwe-y7ZcqTf_bUag2UKo60","1vNj9n2R9BRqq2I5tkmVvhsj2Sa37P_3J9n1WmF8aKcU",
          "1mYyNUJtqLhPdSp6FUdFhp1uKqKeFbZ3899RqUIsGnn4","1RhV6Mk4uR3nUcdbKL8d0J7ABTi9eCRznqDbxDvqasjM",
          "1iLls0p7gYxhvMLdoM929a9W3S1napoVSqnW_3e2qGOA","1bASj60-_TvBX70kSCQw4krqInBbvhPS4_Swz_hY1QiU",
          "1w1e1GtQeJ0LIgt1MVnC8cYu8YlmLnpQub6AYP3BHbYM", "1nSyId3ZL1ptTRkRVfk1GWW-cJN5vUpxF3_WMPp7pb84")
state_keys <- tibble(states,keys)

#Connect to the DB -- CODE REMOVED FOR SECURITY REASONS



raw_data <- dbGetQuery(con, "with last_year as (
select distinct ent.id as id
from extended_lineitems as el 
    join entities as ent on ent.id = el.entity_id
    join jurisdictions as j on j.id = ent.jurisdiction_id
    join lists as l on l.id = el.list_id
where el.refund_id is null 
and el.paid_at is not null
and ent.kind in ('candidate', 'jointcommittee')
and (j.display_name like 'State%' or j.display_name like 'Local%')
and ent.donate = true
and el.paid_at between '01/01/2021' and now() != '0' -- only accounts who have recieved donations within the last 6 months
)

select distinct ent.id as id,
    ent.displayname as name,   
    ent.contactemail,
    case when ent.express_lane_enabled is true then 'Express Lane Enabled' 
    else 'No Express Lane' end as el,
    ent.kind as type,
    coalesce (count(distinct l.branding_id) filter (where l.branding_id is not null), 0) as number_brandings,
    coalesce (ent.url_website, 'Website not listed') as website, 
    ent.created_at::date,
    coalesce (sum(el.amount) filter (where el.cycle = 2020), 0)  as amount_cycle,
    coalesce (sum(el.amount) filter (where el.paid_at between now() - interval '1 month' and now()), 0)  as amount_last_month,
    max(el.paid_at)::date as last_contribution,
    substring (j.display_name from 8 for 2) as state
from extended_lineitems as el 
join entities as ent on ent.id = el.entity_id
join jurisdictions as j on j.id = ent.jurisdiction_id
join lists as l on l.id = el.list_id
where ent.id in (select id from last_year)
and el.refund_id is null 
and el.paid_at is not null
group by ent.displayname, ent.id, ent.express_lane_enabled, ent.contactemail, ent.kind,
    ent.url_website, ent.created_at, j.display_name
order by amount_last_month DESC;")

#Convert Fundraising information to money
raw_data$amount_cycle <- as.numeric(raw_data$amount_cycle)
raw_data$amount_cycle <- dollar(raw_data$amount_cycle, scale = 1/100, accuracy = .01)

raw_data$amount_last_month <- as.numeric(raw_data$amount_last_month)
raw_data$amount_last_month <- dollar(raw_data$amount_last_month, scale = 1/100, accuracy = .01)

i <- 1
update_gs4(state_keys)
update_gs(state_keys)




dbDisconnect(con)
