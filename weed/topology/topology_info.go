package topology

import (
	"github.com/seaweedfs/seaweedfs/weed/rpc/master_pb"
	"golang.org/x/exp/slices"
)

type TopologyInfo struct {
	Max         int64              `json:"Max"`
	Free        int64              `json:"Free"`
	DataCenters []DataCenterInfo   `json:"DataCenters"`
	Layouts     []VolumeLayoutInfo `json:"Layouts"`
}

func (t *Topology) ToInfo() (info TopologyInfo) {
	info.Max = t.diskUsages.GetMaxVolumeCount()
	info.Free = t.diskUsages.FreeSpace()
	var dcs []DataCenterInfo
	for _, c := range t.Children() {
		dc := c.(*DataCenter)
		dcs = append(dcs, dc.ToInfo())
	}

	slices.SortFunc(dcs, func(a, b DataCenterInfo) bool {
		return a.Id < b.Id
	})

	info.DataCenters = dcs
	var layouts []VolumeLayoutInfo
	for _, col := range t.collectionMap.Items() {
		c := col.(*Collection)
		for _, layout := range c.storageType2VolumeLayout.Items() {
			if layout != nil {
				tmp := layout.(*VolumeLayout).ToInfo()
				tmp.Collection = c.Name
				layouts = append(layouts, tmp)
			}
		}
	}
	info.Layouts = layouts
	return
}

func (t *Topology) ToVolumeMap() interface{} {
	m := make(map[string]interface{})
	m["Max"] = t.diskUsages.GetMaxVolumeCount()
	m["Free"] = t.diskUsages.FreeSpace()
	dcs := make(map[NodeId]interface{})
	for _, c := range t.Children() {
		dc := c.(*DataCenter)
		racks := make(map[NodeId]interface{})
		for _, r := range dc.Children() {
			rack := r.(*Rack)
			dataNodes := make(map[NodeId]interface{})
			for _, d := range rack.Children() {
				dn := d.(*DataNode)
				var volumes []interface{}
				for _, v := range dn.GetVolumes() {
					volumes = append(volumes, v)
				}
				dataNodes[d.Id()] = volumes
			}
			racks[r.Id()] = dataNodes
		}
		dcs[dc.Id()] = racks
	}
	m["DataCenters"] = dcs
	return m
}

func (t *Topology) ToVolumeLocations() (volumeLocations []*master_pb.VolumeLocation) {
	for _, c := range t.Children() {
		dc := c.(*DataCenter)
		for _, r := range dc.Children() {
			rack := r.(*Rack)
			for _, d := range rack.Children() {
				dn := d.(*DataNode)
				volumeLocation := &master_pb.VolumeLocation{
					Url:        dn.Url(),
					PublicUrl:  dn.PublicUrl,
					DataCenter: dn.GetDataCenterId(),
				}
				for _, v := range dn.GetVolumes() {
					volumeLocation.NewVids = append(volumeLocation.NewVids, uint32(v.Id))
				}
				for _, s := range dn.GetEcShards() {
					volumeLocation.NewVids = append(volumeLocation.NewVids, uint32(s.VolumeId))
				}
				volumeLocations = append(volumeLocations, volumeLocation)
			}
		}
	}
	return
}

func (t *Topology) ToTopologyInfo() *master_pb.TopologyInfo {
	m := &master_pb.TopologyInfo{
		Id:        string(t.Id()),
		DiskInfos: t.diskUsages.ToDiskInfo(),
	}
	for _, c := range t.Children() {
		dc := c.(*DataCenter)
		m.DataCenterInfos = append(m.DataCenterInfos, dc.ToDataCenterInfo())
	}
	return m
}
