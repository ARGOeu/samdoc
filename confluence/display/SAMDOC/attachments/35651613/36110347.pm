#!/usr/bin/perl -w
#
# Nagios configuration generator (WLCG probe based)
# Copyright (c) 2007 Emir Imamagic
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

package NCG::SiteInfo::ATP;

use strict;
use NCG::SiteInfo;
use LWP::UserAgent;
use JSON;
use Data::Dumper;

use vars qw(@ISA);

@ISA=("NCG::SiteInfo");

my $DEFAULT_ATP_ROOT_URL = "https://grid-monitoring.cern.ch/atp";
my $DEFAULT_X509_CERT = '/etc/grid-security/hostcert.pem';
my $DEFAULT_X509_KEY = '/etc/grid-security/hostkey.pem';
my $DEFAULT_ATP_ROOT_URL_SUFFIX = "/api/search/servicemap/json";
my $DEFAULT_ATP_ROOT_URL_VO_SUFFIX = "/api/search/vofeeds/json";

sub new
{
    my $proto  = shift;
    my $class  = ref($proto) || $proto;
    my $self =  $class->SUPER::new(@_);

    # set default values
    if (! $self->{ATP_ROOT_URL}) {
        $self->{ATP_ROOT_URL} = $DEFAULT_ATP_ROOT_URL;
    }
    if (! $self->{X509_CERT}) {
        $self->{X509_CERT} = $DEFAULT_X509_CERT;
    }
    if (! $self->{X509_KEY}) {
        $self->{X509_KEY} = $DEFAULT_X509_KEY;
    }
    if (! -f $self->{X509_KEY} || ! -r $self->{X509_KEY} ) {
        $self->error("X509 key ($self->{X509_KEY}) does not exist or it is not readable!");
        undef $self;
        return;
    }
    if (! -f $self->{X509_CERT} || ! -r $self->{X509_CERT} ) {
        $self->error("X509 certificate ($self->{X509_CERT}) does not exist or it is not readable!");
        undef $self;
        return;
    }

    $self->{MONITORED} = 'Y' unless (exists $self->{MONITORED});
    $self->{PRODUCTION} = undef unless (exists $self->{PRODUCTION});
    $self->{DELETED} = 'N' unless (exists $self->{DELETED});

    $self->{TIMEOUT} = $self->{DEFAULT_HTTP_TIMEOUT} unless ($self->{TIMEOUT});

    if (! exists $self->{FEED_TYPE}) {
        $self->{FEED_TYPE} = 'generic';
    } else {
        if ($self->{FEED_TYPE} =~ /vo/i) {
            unless ($self->{VO}) {
                $self->error("Parameter VO must be defined if FEED_TYPE is \"vo\".");
                return;
            }
        }
    }

    $self;
}

sub ATPRootUrl {
    my $self = shift;
    my $value = shift;
    $self->{ATP_ROOT_URL} = $value if (defined $value);
    $self->{ATP_ROOT_URL};
}

sub defaultATPRootUrl {
    my $self = shift;
    return $DEFAULT_ATP_ROOT_URL;
}

sub getData
{
    my $self = shift;
    my $sitename = shift || $self->{SITENAME};
    my $url;
    local %ENV = %ENV;

    $ENV{HTTPS_KEY_FILE} = $self->{X509_KEY};
    $ENV{HTTPS_CERT_FILE} = $self->{X509_CERT};
    $ENV{HTTPS_CA_DIR} = '/etc/grid-security/certificates';

    my $ua = LWP::UserAgent->new(timeout=>$self->{TIMEOUT}, env_proxy=>1);
    $ua->agent("NCG::SiteInfo::ATP");

    if ($self->{FEED_TYPE} =~ /vo/i) {
        $url = $self->{ATP_ROOT_URL} . $DEFAULT_ATP_ROOT_URL_VO_SUFFIX . '?vo=' . $self->{VO} . '&atp_site=' . $sitename;
    } else {
        $url = $self->{ATP_ROOT_URL} . $DEFAULT_ATP_ROOT_URL_SUFFIX  . '?site=' . $sitename;
    }
    $url .= '&ismonitored=on' if ($self->{MONITORED} ne 'N');

    my $req = HTTP::Request->new(GET => $url);
    my $res = $self->safeHTTPSCall($ua,$req);

    if (!$res->is_success) {
        $self->error("Could not get results from ATP: ".$res->status_line);
        return;
    }

    my $jsonRef;
    eval {
        $jsonRef = from_json($res->content);
    };
    if ($@) {
        $self->error("Error parsing JSON response: ".$@);
        return;
    }

    if ($jsonRef && ref $jsonRef eq "ARRAY") {
        if ($self->{FEED_TYPE} =~ /vo/i) {
            foreach my $service (@{$jsonRef}) {
                if ($service->{hostname} && $service->{service_flavour} && $service->{vo}->{voname}) {
                    $self->{SITEDB}->addHost($service->{hostname});
                    $self->{SITEDB}->addService($service->{hostname}, $service->{service_flavour});
                    $self->{SITEDB}->addVO($service->{hostname}, $service->{service_flavour}, $service->{vo}->{voname});
                    $self->{SITEDB}->hostAttribute($service->{hostname}, $service->{service_flavour}."_URL", $service->{serviceendpoint}) if ($service->{serviceendpoint});
                }
            }
        } else {
            # unless (@{$jsonRef}) {
            #    $self->error("ATP didn't find any services on site: $sitename. There seems to be a problem with ATP, stopping configuration.");
            #    return;
            # }

            foreach my $service (@{$jsonRef}) {
                next if ($service->{isdeleted} &&
                         $self->{DELETED} &&
                         $service->{isdeleted} ne $self->{DELETED});
                next if ($service->{isinproduction} &&
                         $self->{PRODUCTION} &&
                         $service->{isinproduction} ne $self->{PRODUCTION});
                next if ($service->{ismonitored} &&
                         $self->{MONITORED} &&
                         $service->{ismonitored} ne $self->{MONITORED});

                $self->{SITEDB}->addHost($service->{hostname});

                next if ( ref $service->{flavour} ne "HASH" ||
                          !$service->{flavour}->{flavourname} );
                my $serviceFlavour = $service->{flavour}->{flavourname};
                $self->{SITEDB}->addService($service->{hostname}, $serviceFlavour);
                $self->{SITEDB}->siteLDAP($service->{hostname})
                    if ($serviceFlavour eq 'sBDII' ||
                        $serviceFlavour eq 'Site-BDII');

                $self->{SITEDB}->hostAttribute($service->{hostname}, $serviceFlavour."_URL", $service->{serviceendpoint}) if ($service->{serviceendpoint});

                foreach my $vo (@{$service->{vo}}) {
                    next unless ($vo->{voname});
                    $self->{SITEDB}->addVO($service->{hostname}, $serviceFlavour, $vo->{voname});
                }
            }
        }
    } else {
        $self->error("Invalid JSON format: ".Dumper($jsonRef));
        return;
    }

    1;
}

=head1 NAME

NCG::SiteInfo::ATP

=head1 DESCRIPTION

The NCG::SiteInfo::ATP module extends NCG::SiteInfo module. Module
extracts site information from ATP (eng. Aggregated Topology Provider)
database.

=head1 SYNOPSIS

  use NCG::SiteInfo::ATP;

  my $siteInfo = NCG::SiteInfo::ATP->new({ { SITEDB=> $sitedb } );

  $siteInfo->getData();

=cut

=head1 METHODS

=over

=item C<new>

  $siteInfo = NCG::SiteInfo::ATP->new( $options );

Creates new NCG::SiteInfo::ATP instance. Argument $options is hash
reference that can contain following elements:
  ATP_ROOT_URL - root URL used for ATP DB query interface
  (default: http://grid-monitoring.cern.ch/atp)
  
  FEED_TYPE - defines type of feed, currently only two are supported:
                vo - uses VO query /api/search/vofeeds
                generic - uses generic query /api/search/siteregionvo
            - default: generic

  MONITORED - is node monitored (for possible values see
                   ATP documentation)
  (default: Y)

  PRODUCTION - is node in production (for possible values see
               ATP documentation)
  (default: Y)

  DELETED - checks isdeleted field
  (default: N)
  
  VO - VO used in case of FEED_TYPE vo
     - parameter must be set if FEED_TYPE is vo
     - default:

  TIMEOUT - HTTP timeout,
  (default: DEFAULT_HTTP_TIMEOUT inherited from NCG)

=item C<ATPRootUrl>

  print $ncg->ATPRootUrl;
  $ncg->ATPRootUrl('http://sam.server.com/new-api/');
  
Accessor method for property ATP_ROOT_URL.  

=item C<defaultATPRootUrl>

  print $ncg->ATPRootUrl;

Read only accessor method for default value of property
ATP_ROOT_URL.

=back

=head1 SEE ALSO

NCG::Site

=cut

1;
