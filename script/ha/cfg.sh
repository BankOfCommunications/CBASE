crm_attribute --type crm_config --attr-name symmetric-cluster --attr-value true
crm_attribute --type crm_config --attr-name stonith-enabled --attr-value false
crm_attribute --type rsc_defaults --name resource-stickiness --update 100
cibadmin --replace --obj_type=resources --xml-file ./all.xml
