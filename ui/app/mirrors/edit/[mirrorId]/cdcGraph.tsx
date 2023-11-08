import { Label } from '@/lib/Label';
import { useState, useEffect } from 'react';
import moment from 'moment';

type SyncStatusRow = {
    batchId: number;
    startTime: Date;
    endTime: Date | null;
    numRows: number;
};

import aggregateCountsByInterval from './aggregatedCountsByInterval'

const aggregateTypeMap = {
  "15min":" 15 mins",
  "hour": "Hour",
  "day": "Day",
  "month": "Month"
}

  function CdcGraph({syncs}:{syncs:SyncStatusRow[]}) {

    let [aggregateType,setAggregateType] = useState('hour');
    let [counts,setCounts] = useState([]);


    let rows = syncs.map((sync) => ({
        timestamp: sync.startTime,
        count: sync.numRows,
    }))

    useEffect(()=>{
      let counts = aggregateCountsByInterval(rows,aggregateType, undefined, new Date());
      counts = counts.slice(0,29)
      counts = counts.reverse();
      setCounts(counts)
  
    },[aggregateType, rows])    

    return <div>
            <div className='float-right'>
              {["15min","hour","day","month"].map((type)=>{
                return <FilterButton key={type} aggregateType={type} selectedAggregateType={aggregateType} setAggregateType={setAggregateType}/>
              })}
            </div>
            <div><Label variant="body">Sync history</Label></div>

            <div className='flex space-x-2 justify-left ml-2'>
                {counts.map((count,i)=><GraphBar key={i} label={formatGraphLabel(new Date(count[0]),aggregateType)} count={count[1]}/>)}
            </div>
          </div>
  }

  type filterButtonProps = {  
    aggregateType:String;
    selectedAggregateType:String;
    setAggregateType:Function;
  };
  function FilterButton({aggregateType,selectedAggregateType,setAggregateType}:filterButtonProps){
    return  <button 
              className={aggregateType === selectedAggregateType ? "bg-gray-200 px-1 mx-1 rounded-md":"px-1 mx-1"}
              onClick={()=>setAggregateType(aggregateType)}>
              {aggregateTypeMap[aggregateType]}
            </button>
  }
  
  type GraphBarProps = {
    count: number | undefined;
    label: string
  };
  

  function formatGraphLabel(date:Date, aggregateType:String) {
    switch (aggregateType) {
      case "15min":
        return moment(date).format('MMM Do HH:mm');
      case "hour":
        return moment(date).format('MMM Do HH:mm');
      case "day":
        return moment(date).format('MMM Do');
      case "month":
        return moment(date).format('MMM yy');
    }
  }

  function GraphBar({label,count}:GraphBarProps){
    let color = count && count >0 ? 'bg-green-500' : 'bg-gray-500';
    let classNames = `relative w-10 h-24 rounded  ${color}`;
    return <div className={"group"}>
              <div className={classNames}>
                <div className="group-hover:opacity-100 transition-opacity bg-gray-800 px-1 text-sm text-gray-100 rounded-md absolute left-1/2 
        -translate-x-1/2 translate-y-full opacity-0 m-4 mx-auto w-28 z-10 text-center">
                  <div>{label}</div>
                  <div>{count}</div>
                </div>
              </div>
            </div>
  }


  export default CdcGraph;
