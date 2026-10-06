import ExecutionEnvironment from '@docusaurus/ExecutionEnvironment';

const clarityId = 'loh6v65ww5';
const consentContainerId = 'cookie-banner';
const consentScriptId = 'wcp-consent-script';

let siteConsent;
let initialized = false;

function getAnalyticsConsent() {
  return siteConsent.getConsentFor(window.WcpConsent.consentCategories.Analytics);
}

function updateClarityConsent() {
  if (!siteConsent || typeof window.clarity !== 'function') {
    return;
  }

  window.clarity('consentv2', {
    ad_Storage: 'denied',
    analytics_Storage: getAnalyticsConsent() ? 'granted' : 'denied',
  });
}

function loadClarity() {
  if (typeof window.clarity !== 'function') {
    window.clarity = function() {
      (window.clarity.q = window.clarity.q || []).push(arguments);
    };
  }

  // Queue the consent state before Clarity initializes and accesses cookies.
  updateClarityConsent();

  if (document.querySelector(`script[data-clarity-id="${clarityId}"]`)) {
    return;
  }

  const script = document.createElement('script');
  script.async = true;
  script.dataset.clarityId = clarityId;
  script.src = `https://www.clarity.ms/tag/${clarityId}`;
  script.addEventListener('error', () => {
    console.error('Microsoft Clarity failed to load.');
  });
  document.head.appendChild(script);
}

function ensureConsentContainer() {
  let container = document.getElementById(consentContainerId);
  if (!container) {
    container = document.createElement('div');
    container.id = consentContainerId;
    document.body.insertBefore(container, document.body.firstChild);
  }

  return container;
}

function onConsentChanged() {
  updateClarityConsent();
}

function initializeConsent() {
  if (initialized || !window.WcpConsent) {
    return false;
  }

  initialized = true;
  window.WcpConsent.init(
    'en-US',
    ensureConsentContainer(),
    (error, consent) => {
      if (error) {
        console.error('Error initializing WCP cookie consent:', error);
        return;
      }

      siteConsent = consent;
      document.documentElement.classList.toggle(
        'wcp-consent-required',
        siteConsent.isConsentRequired,
      );
      loadClarity();
    },
    onConsentChanged,
    window.WcpConsent.themes.light,
  );

  return true;
}

function initializeWhenConsentLibraryLoads() {
  const consentScript = document.getElementById(consentScriptId);
  if (!consentScript) {
    console.error('WCP cookie consent script element was not found.');
    return;
  }

  consentScript.addEventListener('load', initializeConsent, {once: true});
  consentScript.addEventListener('error', () => {
    console.error('WCP cookie consent library failed to load.');
  }, {once: true});

  if (initializeConsent()) {
    return;
  }

  window.setTimeout(() => {
    if (!initialized) {
      console.error('WCP cookie consent library did not initialize.');
    }
  }, 10000);
}

function manageConsent(event) {
  if (!(event.target instanceof Element)) {
    return;
  }

  const trigger = event.target.closest('[data-cookie-preferences]');
  if (!trigger) {
    return;
  }

  event.preventDefault();
  if (siteConsent) {
    siteConsent.manageConsent();
  }
}

if (ExecutionEnvironment.canUseDOM) {
  document.addEventListener('click', manageConsent);

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initializeWhenConsentLibraryLoads, {once: true});
  } else {
    initializeWhenConsentLibraryLoads();
  }
}
