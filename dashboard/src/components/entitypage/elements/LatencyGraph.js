import React from "react"
import { ResponsiveBar } from '@nivo/bar'

const LatencyGraph = ({latency}) => {

    let data = {
        10: {'ms':10, requests: 2},
        20: {'ms':20, requests: 4},
        30: {'ms':30, requests: 12},
        40: {'ms':40, requests: 45},
        50: {'ms':50, requests: 35},
        60: {'ms':60, requests: 15},
        70: {'ms':70, requests: 4},
    }

    latency.forEach(value => {
        let tens = Math.ceil(value / 10) * 10
        let prevReqs  = 0
        if(data[tens] ){
            prevReqs = data[tens]['requests']
        }
        data[tens] = {'ms': tens, requests: prevReqs+1}
    })
    

    let values = Object.keys(data).map(function(key){
        return data[key];
    });

    return (
        <ResponsiveBar
        data={values}
        keys={['requests']}
        indexBy="ms"
        margin={{ top: 50, right: 130, bottom: 50, left: 60 }}
        padding={0.1}
        valueScale={{ type: 'linear' }}
        indexScale={{ type: 'band', round: true }}
        valueFormat={{ format: '', enabled: false }}
        colors={{ scheme: 'nivo' }}
        defs={[
            {
                id: 'dots',
                type: 'patternDots',
                background: 'inherit',
                color: '#38bcb2',
                size: 4,
                padding: 1,
                stagger: true
            },
            {
                id: 'lines',
                type: 'patternLines',
                background: 'inherit',
                color: '#eed312',
                rotation: -45,
                lineWidth: 6,
                spacing: 10
            }
        ]}

        borderColor={{ from: 'color', modifiers: [ [ 'darker', 1.6 ] ] }}
        axisTop={null}
        axisRight={null}
        axisBottom={{
            tickSize: 5,
            tickPadding: 5,
            tickRotation: 0,
            legend: 'milliseconds',
            legendPosition: 'middle',
            legendOffset: 32
        }}
        axisLeft={{
            tickSize: 5,
            tickPadding: 5,
            tickRotation: 0,
            legend: 'requests',
            legendPosition: 'middle',
            legendOffset: -40
        }}
        labelSkipWidth={12}
        labelSkipHeight={12}
        labelTextColor={{ from: 'color', modifiers: [ [ 'darker', 1.6 ] ] }}

    />

    )
}
    
export default LatencyGraph