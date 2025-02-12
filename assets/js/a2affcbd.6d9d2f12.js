"use strict";(self.webpackChunkwebsite=self.webpackChunkwebsite||[]).push([[2944],{9371:(e,t,n)=>{n.r(t),n.d(t,{assets:()=>c,contentTitle:()=>s,default:()=>h,frontMatter:()=>i,metadata:()=>a,toc:()=>l});var a=n(6412),o=n(4848),r=n(8453);const i={slug:"brief-history",title:"A Brief History of Garnet",authors:"badrishc",tags:["garnet","history","introduction"]},s=void 0,c={authorsImageUrls:[void 0]},l=[];function d(e){const t={a:"a",p:"p",...(0,r.R)(),...e.components};return(0,o.jsxs)(o.Fragment,{children:[(0,o.jsxs)(t.p,{children:["Hi everyone! I just wanted to start off this blog with a short history of Garnet and how it\ncame to exist. At Microsoft Research, we have been working on storage technology\nfor a while. In 2016, we started working on a new key-value store design based on epoch protection and\na powerful storage API. This project, called ",(0,o.jsx)(t.a,{href:"https://github.com/microsoft/FASTER",children:"FASTER"}),", was\nopen-sourced in 2018 and gained a lot of traction within Microsoft and in the larger community. FASTER\nhas over 6k stars and over half a million NuGet downloads. Over the next several years, we built\nfollow-on capabilities such as recoverability (CPR) and serverless support (Netherite), and the\nproject was widely adopted."]}),"\n",(0,o.jsx)(t.p,{children:"Around 2021, we noticed a huge interest in remote cache-stores, particularly using APIs\nsuch as the RESP API of Redis. Developers loved the flexibility of both the API and the\ndeployment model as a separate process/service. The cost savings compared to accessing\ncloud databases directly drove the adoption of caching layers, and these soon\ngrew to take up a significant portion of the operating cost of large services. When the\npandemic hit and online service usage spiked, there was a strong need for lowering costs\nand improving performance (throughput and latency) for such caching layers."}),"\n",(0,o.jsx)(t.p,{children:"We took on the challenge of building a new system, called Garnet, which could provide\nextremely high performance end-to-end in a client-server setup while allowing clients\nto remain largely unmodified by adopting RESP, the most popular wire protocols out there.\nAfter a lot of design effort, we came up with a server threading model that could indeed\nmake a huge end-to-end difference, often by orders-of-magnitude, in performance for\nbasic get and set operations, with unmodified client code. This gave us the confidence to\nbuild out Garnet's feature set towards use in real scenarios."}),"\n",(0,o.jsx)(t.p,{children:"The next question was API coverage. The Redis API is vast, and we were just a small research\nteam. Thankfully, our stack was built on .NET, which made tremendous advances in both performance\nand richness of libraries. We designed a generic yet powerful way to define and use custom\ndata structures, and were able to quickly implement complex datatypes such as Sorted Set, List,\nand Hash by reusing data structures in C#. We then built complex structures such as HyperLogLog\nand Bitmap as well, and added transactions and extensibility features."}),"\n",(0,o.jsx)(t.p,{children:"The next requirement was scale-out and recovery, for which we designed write-ahead operation logging,\nsharding capability, replication, and key migration for dynamic scale-out. By keeping basic compatibility\nwith the Redis API, we were able to add these features in a way that existing client code could\nbe largely unchanged."}),"\n",(0,o.jsx)(t.p,{children:"After thousands of unit tests and a couple of years working with first-party teams at Microsoft\ndeploying Garnet in production (more on this in future blog posts!), we felt it was time to release\nit publicly, just like we did with FASTER five years back. We wanted developers across the planet to\nbenefit from this powerful technology and contribute back to the codebase as well. We felt that\nthe modern C# codebase would be particularly attractive here, in terms of ease of expansion,\nmaintenance, and contribution. Coming from MSR, we also wanted people in academia to conduct research,\ncollaborate with us, and expand various aspects of the system."}),"\n",(0,o.jsx)(t.p,{children:"So, explore Garnet, see if you can find use for it in your applications, consider helping us out\nwith expanding its capabilities, and as always, let us know what you think!"})]})}function h(e={}){const{wrapper:t}={...(0,r.R)(),...e.components};return t?(0,o.jsx)(t,{...e,children:(0,o.jsx)(d,{...e})}):d(e)}},8453:(e,t,n)=>{n.d(t,{R:()=>i,x:()=>s});var a=n(6540);const o={},r=a.createContext(o);function i(e){const t=a.useContext(r);return a.useMemo((function(){return"function"==typeof e?e(t):{...t,...e}}),[t,e])}function s(e){let t;return t=e.disableParentContext?"function"==typeof e.components?e.components(o):e.components||o:i(e.components),a.createElement(r.Provider,{value:t},e.children)}},6412:e=>{e.exports=JSON.parse('{"permalink":"/garnet/blog/brief-history","editUrl":"https://github.com/microsoft/garnet/tree/main/website/blog/2024-03-17-a-brief-history-of-garnet.md","source":"@site/blog/2024-03-17-a-brief-history-of-garnet.md","title":"A Brief History of Garnet","description":"Hi everyone! I just wanted to start off this blog with a short history of Garnet and how it","date":"2024-03-17T00:00:00.000Z","tags":[{"inline":true,"label":"garnet","permalink":"/garnet/blog/tags/garnet"},{"inline":true,"label":"history","permalink":"/garnet/blog/tags/history"},{"inline":true,"label":"introduction","permalink":"/garnet/blog/tags/introduction"}],"readingTime":2.885,"hasTruncateMarker":false,"authors":[{"name":"Badrish Chandramouli","title":"Partner Research Manager, Microsoft Research","url":"https://badrish.net","imageURL":"https://badrish.net/assets/icons/badrish4.jpg","key":"badrishc","page":null}],"frontMatter":{"slug":"brief-history","title":"A Brief History of Garnet","authors":"badrishc","tags":["garnet","history","introduction"]},"unlisted":false,"prevItem":{"title":"OSS Announcement","permalink":"/garnet/blog/msr-blog-announcement"}}')}}]);