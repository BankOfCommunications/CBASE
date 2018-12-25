def MultiClusterAttr():
    id = 'all: obi id'
    pid = 'all: obi pid'
    stop = 'all: obi force_stop'
    reboot = 'all: obi reboot'
    return locals()

def MultiCluster(**cfg):
    [v.get('name') or v.update(name=k) for k,v in get_match_child(cfg, 'obi').items()]
    obi_list = [v for (k,v) in sorted(get_match_child(cfg, 'obi').items(), key=lambda (k,v): v.get('obi_idx', 0))]
    if not obi_list: raise Fail('no obi defined!')
    obi_list[0].update(obi_role='OBI_MASTER')
    for obi in obi_list[1:]:
        obi.update(obi_role='OBI_SLAVE')
    for idx,obi in enumerate(obi_list):
        obi.update(cluster_id=idx)
    return dict_updated(dict_filter_out_special_attrs(MultiClusterAttr()), mmrs='${%s.rs0.addr}'%(obi_list[0].get('name')), **cfg)

def multi_cluster_install():
    ObCfg.__dict__.update(MultiCluster=MultiCluster)
multi_cluster_install()
