-- Easynews (and potentially other usenet) scrapes persisted nzb_url with
-- embedded HTTP basic-auth credentials and/or sensitive query params because
-- the scrape-time sanitize_nzb_url() step was not applied before INSERT.
-- Scrub any already-stored rows; new rows are sanitized at write time.

-- 1. Strip "user:pass@" basic-auth credentials from the URL authority.
UPDATE usenet_stream
SET nzb_url = regexp_replace(nzb_url, '://[^/@]+@', '://', 'g')
WHERE nzb_url ~ '://[^/@]+@';

-- 2. Strip sensitive query-string parameters.
UPDATE usenet_stream
SET nzb_url = regexp_replace(
        regexp_replace(
            regexp_replace(
                regexp_replace(
                    nzb_url,
                    '[?&](apikey|api_key|token|authorization|auth|passkey|password|pwd|username|user|rsskey|key|secret)=[^&]*',
                    '',
                    'gi'
                ),
                '&&+', '&', 'g'
            ),
            '\?&', '?', 'g'
        ),
        '[?&]$', ''
    )
WHERE nzb_url ~* '[?&](apikey|api_key|token|authorization|auth|passkey|password|pwd|username|user|rsskey|key|secret)=';
