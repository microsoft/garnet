"use strict";(self.webpackChunkwebsite=self.webpackChunkwebsite||[]).push([[7507],{7360:(e,t,n)=>{n.r(t),n.d(t,{assets:()=>l,contentTitle:()=>o,default:()=>d,frontMatter:()=>i,metadata:()=>a,toc:()=>c});var r=n(4848),s=n(8453);const i={id:"overview",sidebar_label:"Overview",title:"Overview of Benchmarking"},o=void 0,a={id:"benchmarking/overview",title:"Overview of Benchmarking",description:"Client Tool",source:"@site/docs/benchmarking/overview.md",sourceDirName:"benchmarking",slug:"/benchmarking/overview",permalink:"/garnet/docs/benchmarking/overview",draft:!1,unlisted:!1,editUrl:"https://github.com/microsoft/garnet/tree/main/website/docs/benchmarking/overview.md",tags:[],version:"current",frontMatter:{id:"overview",sidebar_label:"Overview",title:"Overview of Benchmarking"},sidebar:"garnetDocSidebar",previous:{title:"Compaction",permalink:"/garnet/docs/getting-started/compaction"},next:{title:"Results (Resp.bench)",permalink:"/garnet/docs/benchmarking/results-resp-bench"}},l={},c=[{value:"Client Tool",id:"client-tool",level:2},{value:"BDN Performance metrics",id:"bdn-performance-metrics",level:2}];function h(e){const t={a:"a",h2:"h2",li:"li",ol:"ol",p:"p",strong:"strong",...(0,s.R)(),...e.components};return(0,r.jsxs)(r.Fragment,{children:[(0,r.jsx)(t.h2,{id:"client-tool",children:"Client Tool"}),"\n",(0,r.jsxs)(t.p,{children:["We have evaluated Garnet using a benchmarking client tool we wrote, called Resp.benchmark. The tool is written to carefully control\nvarious parameters, and to avoid client side overheads so that the focus can be on the server. Learn more about the tool ",(0,r.jsx)(t.a,{href:"resp-bench",children:"here"}),"."]}),"\n",(0,r.jsx)(t.h2,{id:"bdn-performance-metrics",children:"BDN Performance metrics"}),"\n",(0,r.jsxs)(t.p,{children:["We have incorporated BDN (",(0,r.jsx)(t.a,{href:"https://benchmarkdotnet.org/",children:"BenchmarkDotNet"}),") to track and monitor performance as part of the Garnet CI (Garnet CI BDN.benchmark). The BDN CI runs on each push to main and logs the metrics from that run for both Windows and Linux."]}),"\n",(0,r.jsxs)(t.p,{children:['The BDN CI first looks at the "Allocated" bytes from that specific run and compares it to what is expected. If that found "Allocated" value is 10% more than the expected "Allocated" value, the BDN CI will fail. If that part of the CI passes, then the "Mean" value is compared to the "Mean" value of the previous run. At this point, the CI also logs the "Mean" value so it can be charted ',(0,r.jsx)(t.a,{href:"https://microsoft.github.io/garnet/charts/",children:"here"}),"."]}),"\n",(0,r.jsx)(t.p,{children:(0,r.jsx)(t.strong,{children:"To add your BDN to Garnet BDN CI"})}),"\n",(0,r.jsxs)(t.ol,{children:["\n",(0,r.jsxs)(t.li,{children:["\n",(0,r.jsx)(t.p,{children:"The GitHub action yml: .github\\workflows\\ci-bdnbenchmark.yml"}),"\n",(0,r.jsx)(t.p,{children:'a) Update the "test" with the addition of your BDN test (~line 45)'}),"\n",(0,r.jsx)(t.p,{children:"b) Do not add BDN.benchmark prefix in your test in this yml file"}),"\n"]}),"\n",(0,r.jsxs)(t.li,{children:["\n",(0,r.jsx)(t.p,{children:"Add the expected results to \\test\\BDNPerfTests\\BDN_Benchmark_Config.json"}),"\n",(0,r.jsx)(t.p,{children:'a) These values are the expected "Allocated" bytes. If the found value is more than 10% it will show the GH action as a Fail.'}),"\n",(0,r.jsx)(t.p,{children:'b) Follow the pattern for the labels of expected values that others have and use "_".'}),"\n",(0,r.jsx)(t.p,{children:"c) If the expected Allocated value for Linux and Windows are different, use the value that is higher."}),"\n"]}),"\n"]}),"\n",(0,r.jsxs)(t.p,{children:[(0,r.jsx)(t.strong,{children:"To run BDN perf tests locally"}),"\ntest/BDNPerfTests/run_bdnperftest.ps1"]}),"\n",(0,r.jsxs)(t.ol,{children:["\n",(0,r.jsxs)(t.li,{children:["\n",(0,r.jsx)(t.p,{children:"Only parameter (optional) is BDN test. For example, Operations.BasicOperations (default), Operations.ObjectOperations, Cluster.ClusterMigrate, Lua.LuaScripts etc"}),"\n"]}),"\n",(0,r.jsxs)(t.li,{children:["\n",(0,r.jsx)(t.p,{children:"It will verify found Allocated vs expected Allocated (giving it 10% tolerance) and show results in the output."}),"\n"]}),"\n",(0,r.jsxs)(t.li,{children:["\n",(0,r.jsx)(t.p,{children:"BDN test results are in test/BDNPerfTests/results/*.results with the summary at end of file."}),"\n"]}),"\n"]})]})}function d(e={}){const{wrapper:t}={...(0,s.R)(),...e.components};return t?(0,r.jsx)(t,{...e,children:(0,r.jsx)(h,{...e})}):h(e)}},8453:(e,t,n)=>{n.d(t,{R:()=>o,x:()=>a});var r=n(6540);const s={},i=r.createContext(s);function o(e){const t=r.useContext(i);return r.useMemo((function(){return"function"==typeof e?e(t):{...t,...e}}),[t,e])}function a(e){let t;return t=e.disableParentContext?"function"==typeof e.components?e.components(s):e.components||s:o(e.components),r.createElement(i.Provider,{value:t},e.children)}}}]);