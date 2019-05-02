import org.onebusaway.gtfs.impl.GtfsDaoImpl;
import org.onebusaway.gtfs.model.Agency;
import org.onebusaway.gtfs.model.AgencyAndId;
import org.onebusaway.gtfs.model.Route;
import org.onebusaway.gtfs.model.Trip;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public class GtfsMerger {

    private Set<AgencyAndId> mergedRouteIds = new HashSet<>();
    private Set<AgencyAndId> mergedTripIds = new HashSet<>();
    private Set<AgencyAndId> mergedStopTimesIds = new HashSet<>();

    public GtfsDaoImpl merge(GtfsDaoImpl priorityGtfs, GtfsDaoImpl otherGtfs){
        mergeAgencies(priorityGtfs, otherGtfs);
        mergeRoutes(priorityGtfs, otherGtfs);
        mergeTrips(priorityGtfs, otherGtfs);
        mergeStopTimes(priorityGtfs, otherGtfs);
        return priorityGtfs;
    }

    private void mergeAgencies(GtfsDaoImpl priorityGtfs, GtfsDaoImpl otherGtfs){
        for(Agency agency : otherGtfs.getAllAgencies()){
            agency.setId("OTHER_" + agency.getId());
            priorityGtfs.saveEntity(agency);
        }
    }

    private void mergeRoutes(GtfsDaoImpl priorityGtfs, GtfsDaoImpl otherGtfs){
        for(Route route : otherGtfs.getAllRoutes()){
            Route cittatiRoute = findRoute(route, priorityGtfs.getAllRoutes());
            if(cittatiRoute == null){
                route.getId().setId("OTHER_" + route.getId().getId());
                priorityGtfs.saveEntity(route);

                mergedRouteIds.add(route.getId());
            }
        }
    }

    private Route findRoute(Route route, Collection<Route> list){
        for(Route routeFromList : list){
            if(route.getShortName().equals(routeFromList.getShortName())){
                return route;
            }
        }
        return null;
    }

    private void mergeTrips(GtfsDaoImpl priorityGtfs, GtfsDaoImpl otherGtfs){
        for(Trip trip : otherGtfs.getAllTrips()){
            for(AgencyAndId routeId : mergedRouteIds){
                if(trip.getRoute().getId().compareTo(routeId) == 0){
                    trip.getId().setId("OTHER_" + trip.getId().getId());
                    priorityGtfs.saveEntity(trip);
                    mergedTripIds.add(trip.getId());
                    break;
                }
            }
        }
    }

    private void mergeStopTimes(GtfsDaoImpl priorityGtfs, GtfsDaoImpl otherGtfs){
        
    }
}
