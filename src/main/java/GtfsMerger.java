import org.onebusaway.gtfs.impl.GtfsDaoImpl;
import org.onebusaway.gtfs.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class GtfsMerger {

    private static final Logger LOGGER = LoggerFactory.getLogger(GtfsMerger.class);

    private Set<AgencyAndId> serviceIds = new CopyOnWriteArraySet<>();
    private Set<AgencyAndId> shapeIds = new CopyOnWriteArraySet<>();
    private Set<AgencyAndId> mergedRouteIds = new CopyOnWriteArraySet<>();
    private Set<AgencyAndId> mergedTripIds = new CopyOnWriteArraySet<>();

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
        LOGGER.info("Merging agencies");
        for(Agency agency : otherGtfs.getAllAgencies()){
            agency.setId("OTHER_" + agency.getId());
            priorityGtfs.saveEntity(agency);
        }
    }

    private void mergeRoutes(GtfsDaoImpl priorityGtfs, GtfsDaoImpl otherGtfs){
        LOGGER.info("Merging routes");
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
        LOGGER.info("Merging trips");
        ExecutorService exec = Executors.newFixedThreadPool(8);
        List<Callable<Void>> tasks = new ArrayList<Callable<Void>>();
        for(Trip trip : otherGtfs.getAllTrips()){
            Callable<Void> c = new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    for(AgencyAndId routeId : mergedRouteIds){
                        if(trip.getRoute().getId().compareTo(routeId) == 0){
                            trip.getId().setId("OTHER_" + trip.getId().getId());
                            priorityGtfs.saveEntity(trip);
                            mergedTripIds.add(trip.getId());
                            serviceIds.add(trip.getServiceId());
                            if(trip.getShapeId() != null) {
                                shapeIds.add(trip.getShapeId());
                            }
                            break;
                        }
                    }
                    return null;
                }
            };
            tasks.add(c);
        }

        LOGGER.debug("Executing tasks");
        try {
            exec.invokeAll(tasks);
        }catch(InterruptedException ex){
            ex.printStackTrace();
        }
        exec.shutdown();
        try{
            exec.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        }catch(InterruptedException ex){
            ex.printStackTrace();
        }
        LOGGER.debug("Finished tasks");
    }

    private void mergeStopTimes(GtfsDaoImpl priorityGtfs, GtfsDaoImpl otherGtfs){
        LOGGER.info("Merging stop times");
        ExecutorService exec = Executors.newFixedThreadPool(8);
        List<Callable<Void>> tasks = new ArrayList<Callable<Void>>();
        AtomicInteger n = new AtomicInteger(0);
        int total = otherGtfs.getAllStopTimes().size();

        for(StopTime stopTime : otherGtfs.getAllStopTimes()){
            Callable<Void> c = new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    int current = n.incrementAndGet();
                    if(current % 100 == 0) {
                        LOGGER.info("Processing " + current + " of " + total + " stop times");
                    }
                    for(AgencyAndId tripId : mergedTripIds){
                        if(stopTime.getTrip().getId().compareTo(tripId) == 0){
                            stopTime.setId(stopTime.getId() + 4200000);
                            priorityGtfs.saveEntity(stopTime);
                            break;
                        }
                    }
                    return null;
                }
            };
            tasks.add(c);
        }

        LOGGER.debug("Executing tasks");
        try {
            exec.invokeAll(tasks);
        }catch(InterruptedException ex){
            ex.printStackTrace();
        }
        exec.shutdown();
        try{
            exec.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        }catch(InterruptedException ex){
            ex.printStackTrace();
        }
        LOGGER.debug("Finished tasks");
    }

    private void mergeShapeIds(GtfsDaoImpl priorityGtfs, GtfsDaoImpl otherGtfs){
        LOGGER.info("Merging shapes");
        ExecutorService exec = Executors.newFixedThreadPool(8);
        List<Callable<Void>> tasks = new ArrayList<Callable<Void>>();
        AtomicInteger n = new AtomicInteger(0);
        int total = otherGtfs.getAllShapePoints().size();

        for(ShapePoint shapePoint : otherGtfs.getAllShapePoints()){
            Callable<Void> c = new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    int current = n.incrementAndGet();
                    if(current % 100 == 0) {
                        LOGGER.info("Processing " + n + " of " + total + " shape points");
                    }
                    for(AgencyAndId shapeId : shapeIds){
                        if(shapePoint.getShapeId().compareTo(shapeId) == 0){
                            shapePoint.setId(shapePoint.getId() + 4200000);
                            priorityGtfs.saveEntity(shapePoint);
                            break;
                        }
                    }
                    return null;
                }
            };
            tasks.add(c);
        }

        LOGGER.debug("Executing tasks");
        try {
            exec.invokeAll(tasks);
        }catch(InterruptedException ex){
            ex.printStackTrace();
        }
        exec.shutdown();
        try{
            exec.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        }catch(InterruptedException ex){
            ex.printStackTrace();
        }
        LOGGER.debug("Finished tasks");
    }

    private void mergeCalendars(GtfsDaoImpl priorityGtfs, GtfsDaoImpl otherGtfs){
        LOGGER.info("Merging calendars");
        for(ServiceCalendar calendar : otherGtfs.getAllCalendars()){
            for(AgencyAndId serviceId : serviceIds){
                if(calendar.getServiceId().compareTo(serviceId) == 0){
                    calendar.setId(calendar.getId() + 4200000);
                    priorityGtfs.saveEntity(calendar);
                    break;
                }
            }
        }

        LOGGER.info("Merging calendar dates");
        for(ServiceCalendarDate calendarDate : otherGtfs.getAllCalendarDates()){
            for(AgencyAndId serviceId : serviceIds){
                if(calendarDate.getServiceId().compareTo(serviceId) == 0){
                    calendarDate.setId(calendarDate.getId() + 4200000);
                    priorityGtfs.saveEntity(calendarDate);
                    break;
                }
            }
        }
    }

    private void mergeFrequencies(GtfsDaoImpl priorityGtfs, GtfsDaoImpl otherGtfs){
        LOGGER.info("Merging frequencies");
        for(Frequency frequency : otherGtfs.getAllFrequencies()){
            for(AgencyAndId tripId : mergedTripIds){
                if(frequency.getTrip().getId().compareTo(tripId) == 0){
                    frequency.setId(frequency.getId() + 4200000);
                    priorityGtfs.saveEntity(frequency);
                    break;
                }
            }
        }
    }

    private void mergeFares(GtfsDaoImpl priorityGtfs, GtfsDaoImpl otherGtfs){
        LOGGER.info("Merging fares");
        for(FareRule rule : otherGtfs.getAllFareRules()){
            for(AgencyAndId routeId : mergedRouteIds){
                if(rule.getRoute().getId().compareTo(routeId) == 0){
                    rule.setId(rule.getId() + 4200000);
                    if( ! rule.getFare().getId().getId().startsWith("OTHER_")){
                        rule.getFare().getId().setId("OTHER_" + rule.getFare().getId().getId());
                    }
                    priorityGtfs.saveEntity(rule.getFare());
                    priorityGtfs.saveEntity(rule);
                    break;
                }
            }
        }
    }

}
