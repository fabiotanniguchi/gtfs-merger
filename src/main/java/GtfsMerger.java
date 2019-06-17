import org.onebusaway.gtfs.impl.GtfsDaoImpl;
import org.onebusaway.gtfs.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class GtfsMerger {

    private static final Logger LOGGER = LoggerFactory.getLogger(GtfsMerger.class);
    private static final String PREFIX = "OTHER_";
    private static final String TIMEZONE = "America/Recife";

    private Set<AgencyAndId> serviceIds = new CopyOnWriteArraySet<>();
    private Set<AgencyAndId> shapeIds = new CopyOnWriteArraySet<>();
    private Set<AgencyAndId> mergedRouteIds = new CopyOnWriteArraySet<>();
    private Set<AgencyAndId> mergedTripIds = new CopyOnWriteArraySet<>();
    private Set<AgencyAndId> usedStops = new CopyOnWriteArraySet<>();

    public GtfsDaoImpl merge(GtfsDaoImpl priorityGtfs, GtfsDaoImpl otherGtfs){
        mergeAgencies(priorityGtfs, otherGtfs);
        mergeRoutes(priorityGtfs, otherGtfs);
        mergeTrips(priorityGtfs, otherGtfs);
        mergeStopTimes(priorityGtfs, otherGtfs);
        mergeStops(priorityGtfs, otherGtfs);
        mergeShapeIds(priorityGtfs, otherGtfs);
        mergeCalendars(priorityGtfs, otherGtfs);
        mergeFrequencies(priorityGtfs, otherGtfs);
        mergeFares(priorityGtfs, otherGtfs);
        return priorityGtfs;
    }

    private void mergeAgencies(GtfsDaoImpl priorityGtfs, GtfsDaoImpl otherGtfs){
        LOGGER.info("Merging agencies");
        List<Agency> agencies = new LinkedList<>();
        for(Agency agency : priorityGtfs.getAllAgencies()){
            agency.setTimezone(TIMEZONE);
            agencies.add(agency);
            //priorityGtfs.saveOrUpdateEntity(agency);
        }
        for(Agency agency : agencies){
            priorityGtfs.saveOrUpdateEntity(agency);
        }
        for(Agency agency : otherGtfs.getAllAgencies()){
            agency.setId(PREFIX + agency.getId());
            agency.setTimezone(TIMEZONE);
            priorityGtfs.saveOrUpdateEntity(agency);
        }
    }

    private void mergeRoutes(GtfsDaoImpl priorityGtfs, GtfsDaoImpl otherGtfs){
        LOGGER.info("Merging routes");
        for(Route route : otherGtfs.getAllRoutes()){
            Route cittatiRoute = findRoute(route, priorityGtfs.getAllRoutes());
            if(cittatiRoute == null){
                if( ! route.getId().getId().startsWith(PREFIX)) {
                    route.getId().setId(PREFIX + route.getId().getId());
                }
                if(! route.getAgency().getId().startsWith(PREFIX)){
                    route.getAgency().setId(PREFIX + route.getAgency().getId());
                }
                priorityGtfs.saveOrUpdateEntity(route);
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
            if( ! trip.getRoute().getId().getId().startsWith(PREFIX)) {
                trip.getRoute().getId().setId(PREFIX + trip.getRoute().getId().getId());
            }


            Callable<Void> c = new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    for(AgencyAndId routeId : mergedRouteIds){
                        if(trip.getRoute().getId().compareTo(routeId) == 0){
                            trip.getId().setId(PREFIX + trip.getId().getId());
                            if(trip.getShapeId() != null && !trip.getShapeId().getId().startsWith(PREFIX)){
                                trip.getShapeId().setId(PREFIX + trip.getShapeId().getId());
                            }
                            if( ! trip.getServiceId().getId().startsWith(PREFIX)) {
                                trip.getServiceId().setId(PREFIX + trip.getServiceId().getId());
                            }
                            priorityGtfs.saveOrUpdateEntity(trip);
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
            if( ! stopTime.getTrip().getId().getId().startsWith(PREFIX)){
                stopTime.getTrip().getId().setId(PREFIX + stopTime.getTrip().getId().getId());
            }

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
                            if( ! stopTime.getStop().getId().getId().startsWith(PREFIX)){
                                stopTime.getStop().getId().setId(PREFIX + stopTime.getStop().getId().getId());
                            }
                            priorityGtfs.saveOrUpdateEntity(stopTime);
                            usedStops.add(stopTime.getStop().getId());
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
            if( ! shapePoint.getShapeId().getId().startsWith(PREFIX)) {
                shapePoint.getShapeId().setId(PREFIX + shapePoint.getShapeId().getId());
            }

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
                            priorityGtfs.saveOrUpdateEntity(shapePoint);
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
            if( ! calendar.getServiceId().getId().startsWith(PREFIX)) {
                calendar.getServiceId().setId(PREFIX + calendar.getServiceId().getId());
            }
            for(AgencyAndId serviceId : serviceIds){
                if(calendar.getServiceId().compareTo(serviceId) == 0){
                    calendar.setId(calendar.getId() + 4200000);
                    priorityGtfs.saveOrUpdateEntity(calendar);
                    break;
                }
            }
        }

        LOGGER.info("Merging calendar dates");
        for(ServiceCalendarDate calendarDate : otherGtfs.getAllCalendarDates()){
            if( ! calendarDate.getServiceId().getId().startsWith(PREFIX)) {
                calendarDate.getServiceId().setId(PREFIX + calendarDate.getServiceId().getId());
            }
            for(AgencyAndId serviceId : serviceIds){
                if(calendarDate.getServiceId().compareTo(serviceId) == 0){
                    calendarDate.setId(calendarDate.getId() + 4200000);
                    priorityGtfs.saveOrUpdateEntity(calendarDate);
                    break;
                }
            }
        }
    }

    private void mergeFrequencies(GtfsDaoImpl priorityGtfs, GtfsDaoImpl otherGtfs){
        LOGGER.info("Merging frequencies");
        for(Frequency frequency : otherGtfs.getAllFrequencies()){
            if( ! frequency.getTrip().getId().getId().startsWith(PREFIX)){
                frequency.getTrip().getId().setId(PREFIX + frequency.getTrip().getId().getId());
            }
            for(AgencyAndId tripId : mergedTripIds){
                if(frequency.getTrip().getId().compareTo(tripId) == 0){
                    frequency.setId(frequency.getId() + 4200000);
                    priorityGtfs.saveOrUpdateEntity(frequency);
                    break;
                }
            }
        }
    }

    private void mergeFares(GtfsDaoImpl priorityGtfs, GtfsDaoImpl otherGtfs){
//        LOGGER.info("Merging fares");
//        for(FareRule rule : otherGtfs.getAllFareRules()){
//            for(AgencyAndId routeId : mergedRouteIds){
//                if(rule.getRoute().getId().compareTo(routeId) == 0){
//                    rule.setId(rule.getId() + 4200000);
//                    if( ! rule.getRoute().getId().getId().startsWith(PREFIX)) {
//                        rule.getRoute().getId().setId(PREFIX + rule.getRoute().getId().getId());
//                    }
//                    if( ! rule.getFare().getId().getId().startsWith(PREFIX)){
//                        rule.getFare().getId().setId(PREFIX + rule.getFare().getId().getId());
//                    }
//                    priorityGtfs.saveOrUpdateEntity(rule.getFare());
//                    priorityGtfs.saveOrUpdateEntity(rule);
//                    break;
//                }
//            }
//        }
    }

    private void mergeStops(GtfsDaoImpl priorityGtfs, GtfsDaoImpl otherGtfs){
        LOGGER.info("Merging stops");
        ExecutorService exec = Executors.newFixedThreadPool(8);
        List<Callable<Void>> tasks = new ArrayList<Callable<Void>>();

        for(Stop stop : otherGtfs.getAllStops()){
            Callable<Void> c = new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    if( ! stop.getId().getId().startsWith(PREFIX)) {
                        stop.getId().setId(PREFIX + stop.getId().getId());
                    }
                    for(AgencyAndId stopId : usedStops) {
                        if(stop.getId().compareTo(stopId) == 0){
                            priorityGtfs.saveOrUpdateEntity(stop);
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

}
