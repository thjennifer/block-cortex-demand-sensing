
constant: CONNECTION_NAME {
  value: "cortex-demo-thjennifer1"
  export: override_required
}

constant: GCP_PROJECT {
  value: "thjennifer1"
  export: override_required
}

constant: REPORTING_DATASET {
  value: "SAP_REPORTING"
  export: override_required
}

constant: CLIENT {
  value: "900"
  export: override_required
}

constant: years_of_past_data {
   value: "1"
   export: override_required
}


##### for demo data replace current_date() with (SELECT MAX(SalesOrders.RequestedDeliveryDate_VDATU) FROM `@{GCP_PROJECT}.@{REPORTING_DATASET}.SalesOrders`)

constant: use_for_current_date {
  value: "(SELECT MAX(RequestedDeliveryDate_VDATU) FROM `@{GCP_PROJECT}.@{REPORTING_DATASET}.SalesOrders`)"
}
