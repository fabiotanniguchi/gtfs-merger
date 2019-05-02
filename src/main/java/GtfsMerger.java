import org.onebusaway.gtfs.impl.GtfsDaoImpl;
import org.onebusaway.gtfs.model.*;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public class GtfsMerger {

    private Set<AgencyAndId> serviceIds = new HashSet<>();
    private Set<AgencyAndId> shapeIds = new HashSet<>();
    private Set<AgencyAndId> mergedRouteIds = new HashSet<>();
    private Set<AgencyAndId> mergedTripIds = new HashSet<>();

    public GtfsDaoImpl merge(GtfsDaoImpl priorityGtfs, GtfsDaoImpl otherGtfs){
        mergeAgencies(priorityGtfs, otherGtfs);
        mergeRoutes(priorityGtfs, otherGtfs);
        mergeTrips(priorityGtfs, otherGtfs);
        mergeStopTimes(priorityGtfs, otherGtfs);
        mergeShapeIds(priorityGtfs, otherGtfs);
        mergeCalendars(priorityGtfs, otherGtfs);
        mergeFrequencies(priorityGtfs, otherGtfs);
        mergeFares(priorityGtfs, otherGtfs);
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
                //route.getId().setId("OTHER_" + route.getId().getId());
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
                    //trip.getId().setId("OTHER_" + trip.getId().getId());
                    priorityGtfs.saveEntity(trip);
                    mergedTripIds.add(trip.getId());
                    serviceIds.add(trip.getServiceId());
                    if(trip.getShapeId() != null) {
                        shapeIds.add(trip.getShapeId());
                    }
                    break;
                }
            }
        }
    }

    private void mergeStopTimes(GtfsDaoImpl priorityGtfs, GtfsDaoImpl otherGtfs){
        for(StopTime stopTime : otherGtfs.getAllStopTimes()){
            for(AgencyAndId tripId : mergedTripIds){
                if(stopTime.getTrip().getId().compareTo(tripId) == 0){
                    priorityGtfs.saveEntity(stopTime);
                    break;
                }
            }
        }
    }

    private void mergeShapeIds(GtfsDaoImpl priorityGtfs, GtfsDaoImpl otherGtfs){
        for(ShapePoint shapePoint : otherGtfs.getAllShapePoints()){
            for(AgencyAndId shapeId : shapeIds){
                if(shapePoint.getShapeId().compareTo(shapeId) == 0){
                    priorityGtfs.saveEntity(shapePoint);
                    break;
                }
            }
        }
    }

    private void mergeCalendars(GtfsDaoImpl priorityGtfs, GtfsDaoImpl otherGtfs){
        for(ServiceCalendar calendar : otherGtfs.getAllCalendars()){
            for(AgencyAndId serviceId : serviceIds){
                if(calendar.getServiceId().compareTo(serviceId) == 0){
                    priorityGtfs.saveEntity(calendar);
                    break;
                }
            }
        }

        for(ServiceCalendarDate calendarDate : otherGtfs.getAllCalendarDates()){
            for(AgencyAndId serviceId : serviceIds){
                if(calendarDate.getServiceId().compareTo(serviceId) == 0){
                    priorityGtfs.saveEntity(calendarDate);
                    break;
                }
            }
        }
    }

    private void mergeFrequencies(GtfsDaoImpl priorityGtfs, GtfsDaoImpl otherGtfs){
        for(Frequency frequency : otherGtfs.getAllFrequencies()){
            for(AgencyAndId tripId : mergedTripIds){
                if(frequency.getTrip().getId().compareTo(tripId) == 0){
                    priorityGtfs.saveEntity(frequency);
                    break;
                }
            }
        }
    }

    private void mergeFares(GtfsDaoImpl priorityGtfs, GtfsDaoImpl otherGtfs){
        for(FareRule rule : otherGtfs.getAllFareRules()){
            for(AgencyAndId routeId : mergedRouteIds){
                if(rule.getRoute().getId().compareTo(routeId) == 0){
                    priorityGtfs.saveEntity(rule.getFare());
                    priorityGtfs.saveEntity(rule);
                    break;
                }
            }
        }
    }

}
