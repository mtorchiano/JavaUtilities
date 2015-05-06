package it.polito.utility.stream;

import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Stream;

/**
 * 
 * @author Marco Torchiano
 * @version 1.0
 */
public class Ranking {
	
	/**
	 * 
	 * @param propertyExtractor
	 * @param propertyComparator
	 * @return
	 */
	static <T,V> Collector<T,?,SortedMap<Integer,List<T>>> 
	rankingCollector(Function<T,V> propertyExtractor, Comparator<V> propertyComparator){
		return collectingAndThen(	
				groupingBy(propertyExtractor,
						   ()->new TreeMap<>(propertyComparator),
						   toList()),
			    map -> map.entrySet().stream().collect(
			    		 ()->new TreeMap<Integer,List<T>>(){
							private static final long serialVersionUID = 1L;
							public String toString(){
			    				 return entrySet().stream()
			    						  .map( e -> (Stream<String>)e.getValue().stream().map( 
			    					               v -> e.getKey().toString() + " - " + v.toString() 
			    					                   + " (" + propertyExtractor.apply(v) + ")" ) )
			    						 .flatMap(Function.identity())
			    						 .collect(joining("\n")); 
			    			 }
			    		 },
				     (rank,entry) -> 
					 	rank.put(rank.isEmpty()?1:rank.lastKey()+
			  				      		rank.get(rank.lastKey()).size(),
			  				        entry.getValue() ),
			  		(rank1,rank2) -> { 
						 int lastRanking = rank1.lastKey();
						 int offset = lastRanking + rank1.get(lastRanking).size()-1;
						 if( propertyExtractor.apply(rank1.get(lastRanking).get(0))
						     == propertyExtractor.apply(rank2.get(rank2.firstKey()).get(0)) ){
							 rank1.get(lastRanking).addAll(rank2.get(rank2.firstKey()));
							 rank2.remove(rank2.firstKey());
						 }
						 rank2.forEach((r,items) -> {rank1.put(offset+r, items);} );
			  		 }
				)
		);

	}
	
	/**
	 * 
	 * @param stream
	 * @param propertyExtractor
	 * @param propertyComparator
	 * @return
	 */
	static <T,V> SortedMap<Integer,List<T>> 
	rank(Stream<T> stream, Function<T,V> propertyExtractor, Comparator<V> propertyComparator){
		return
		stream.sorted(comparing(propertyExtractor,propertyComparator))
		 .collect(TreeMap::new, 
				 (rank, item) -> {
					 V property = propertyExtractor.apply(item);
					 if(rank.isEmpty()){
						 rank.put(new Integer(1),new LinkedList<T>());
					 }else{
						 Integer r = rank.lastKey();
						 List<T> items = rank.get(r);
						 if(! property.equals(propertyExtractor.apply(items.get(0)))) {
							 rank.put(r+items.size(), new LinkedList<T>());
						 }
					 }
					 rank.get(rank.lastKey()).add(item);
				 },
				 (rank1, rank2) -> { 
					 int lastRanking = rank1.lastKey();
					 int offset = lastRanking + rank1.get(lastRanking).size()-1;
					 if( propertyExtractor.apply(rank1.get(lastRanking).get(0))
					     == propertyExtractor.apply(rank2.get(rank2.firstKey()).get(0)) ){
						 rank1.get(lastRanking).addAll(rank2.get(rank2.firstKey()));
						 rank2.remove(rank2.firstKey());
					 }
					 rank2.forEach((r,items) -> {rank1.put(offset+r, items);} );
				 }
		);
	}

}
