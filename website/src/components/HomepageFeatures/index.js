import clsx from 'clsx';
import Heading from '@theme/Heading';
import styles from './styles.module.css';

const FeatureList = [
  {
    title: 'High Performance',
    Svg: require('@site/static/img/undraw_performance_overview_re_mqrq.svg').default,
    description: (
      <>
	Garnet uses a thread-scalable storage layer called Tsavorite, and provides cache-friendly shared-memory 
        scalability with tiered storage support. Garnet supports cluster mode (sharding and replication). It has
	a fast pluggable network design to get high end-to-end performance (throughput and 99th percentile latency). Garnet
	can reduce costs for large services.
      </>
    ),
  },
  {
    title: 'Rich & Extensible',
    Svg: require('@site/static/img/undraw_scrum_board_re_wk7v.svg').default,
    description: (
      <>
        Garnet uses the popular RESP wire protocol, allowing it to be used with unmodified Redis clients in any language. Garnet supports a large
	fraction of the Redis API surface, including raw strings and complex data structures such as sorted sets, bitmaps, and HyperLogLog. Garnet 
	also has scalable extensibility and transactional stored procedure capabilities.
      </>
    ),
  },
  {
    title: 'Modern & Secure',
    Svg: require('@site/static/img/undraw_secure_login_pdn4.svg').default,
    description: (
      <>
        The Garnet server is written in modern .NET C#, and runs efficiently on almost any platform. It works equally well on Windows and Linux, and is designed to not
	incur garbage collection overheads. You can also extend Garnet's capabilities using new .NET data structures to go beyond the core API. Finally, Garnet has 
        efficient TLS support out of the box.
      </>
    ),
  },
];

function Feature({Svg, title, description}) {
  return (
    <div className={clsx('col col--4')}>
      <div className="text--center">
        <Svg className={styles.featureSvg} role="img" title={title} />
      </div>
      <div className="text--center padding-horiz--md">
        <Heading as="h3">{title}</Heading>
        <p>{description}</p>
      </div>
    </div>
  );
}

export default function HomepageFeatures() {
  return (
    <section className={styles.features}>
      <div className="container">
        <div className="row">
          {FeatureList.map((props, idx) => (
            <Feature key={idx} {...props} />
          ))}
        </div>
      </div>
    </section>
  );
}
