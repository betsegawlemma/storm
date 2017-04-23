/* -----------------------------------------------------------------------
   Process AIS messages read from stdin and output JSON structure
   Copyright 2006 by Brian C. Lane
   All Rights Reserved
   ----------------------------------------------------------------------- */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "portable.h"
#include "nmea.h"
#include "sixbit.h"
#include "vdm_parse.h"


int main( int argc, char *argv[] )
{
    ais_state     ais;
    char          buf[256];

    /* AIS message structures, only parse ones with positions */
    aismsg_1  msg_1;
    aismsg_2  msg_2;
    aismsg_3  msg_3;
    aismsg_4  msg_4;
    aismsg_5  msg_5;
    aismsg_9  msg_9;
    aismsg_11 msg_11;
    aismsg_18 msg_18;
    aismsg_19 msg_19;
    aismsg_21 msg_21;
    aismsg_24 msg_24;
    
    /* Position in DD.DDDDDD */
    double lat_dd;
    double long_ddd;
    long   userid;
    int sog;
    int cog;
    int year;
    char month;
    char day;
    char hour;
    char minute;
    char second;
    char name[21];
    timetag datetime;
    unsigned int i;
    
    /* Clear out the structures */
    memset( &ais, 0, sizeof( ais_state ) );

    
    /* Process incoming packets from stdin */
    while( !feof(stdin) )
    {
        if (fgets( buf, 255, stdin ) == NULL ) break;

        if (assemble_vdm( &ais, buf ) == 0)
        {
            /* Get the 6 bit message id */
            ais.msgid = (unsigned char) get_6bit( &ais.six_state, 6 );
            
            /* process message with appropriate parser */
            switch( ais.msgid ) {
                case 1:
                    if( parse_ais_1( &ais, &msg_1 ) == 0 )
                    {
                        userid = msg_1.userid;
                        pos2ddd( msg_1.latitude, msg_1.longitude, &lat_dd, &long_ddd );
                        sog = msg_1.sog;
                        cog = msg_1.cog;
                        second = msg_1.utc_sec;
                    }
                    break;
                            
                case 2:
                    if( parse_ais_2( &ais, &msg_2 ) == 0 )
                    {
                        userid = msg_2.userid;
                        pos2ddd( msg_2.latitude, msg_2.longitude, &lat_dd, &long_ddd );
			sog = msg_2.sog;
                        cog = msg_2.cog;
			second = msg_2.utc_sec;

                    }
                    break;
                            
                case 3:
                    if( parse_ais_3( &ais, &msg_3 ) == 0 )
                    {
                        userid = msg_3.userid;
                        pos2ddd( msg_3.latitude, msg_3.longitude, &lat_dd, &long_ddd );
			sog = msg_3.sog;
                        cog = msg_3.cog;
			second = msg_3.utc_sec;

                    }
                    break;
                            
                case 4:
                    if( parse_ais_4( &ais, &msg_4 ) == 0 )
                    {
                        userid = msg_4.userid;
                        pos2ddd( msg_4.latitude, msg_4.longitude, &lat_dd, &long_ddd );
                        year = msg_4.utc_year;
			month = msg_4.utc_month;
			day = msg_4.utc_day;
 			hour = msg_4.utc_hour;
                        minute = msg_4.utc_minute;
                        second = msg_4.utc_second;
                        
			
                    }
                    break;
                case 5:
                    if( parse_ais_5( &ais, &msg_5 ) == 0 )
                    {
                        userid = msg_5.userid;
                        i = 0;
                        while (i != 20){
			name[i] = msg_5.name[i];
                        i++;
                        }
                    }
                    break;
           
                case 9:
                    if( parse_ais_9( &ais, &msg_9 ) == 0 )
                    {
                        userid = msg_9.userid;
                        pos2ddd( msg_9.latitude, msg_9.longitude, &lat_dd, &long_ddd );
			get_timetag(&ais.six_state, &datetime);
			sog = msg_9.sog;
                        cog = msg_9.cog;


                    }
                    break;
                case 11:
                    if( parse_ais_11( &ais, &msg_11 ) == 0 )
                    {
                        userid = msg_11.userid;
			year = msg_11.utc_year;
			month = msg_11.utc_month;
			day = msg_11.utc_day;
 			hour = msg_11.utc_hour;
                        minute = msg_11.utc_minute;
                        second = msg_11.utc_second;

                    }
                    break;
   
                case 18:
                    if( parse_ais_18( &ais, &msg_18 ) == 0 )
                    {
                        userid = msg_18.userid;
                        pos2ddd( msg_18.latitude, msg_18.longitude, &lat_dd, &long_ddd );
			sog = msg_18.sog;
                        cog = msg_18.cog;
                        second = msg_18.utc_sec;
                    }
                    break;
                case 19:
                    if( parse_ais_19( &ais, &msg_19 ) == 0 )
                    {
                        userid = msg_19.userid;
                        pos2ddd( msg_19.latitude, msg_19.longitude, &lat_dd, &long_ddd );
                        second = msg_19.utc_sec;
			i = 0;
                        while (i != 20){
			name[i] = msg_19.name[i];
                        i++;
                        }

                    }
                    break;
  		case 21:
                    if( parse_ais_21( &ais, &msg_21 ) == 0 )
                    {
                        userid = msg_21.userid;
                        pos2ddd( msg_21.latitude, msg_21.longitude, &lat_dd, &long_ddd );
                        second = msg_21.utc_sec;
			i = 0;
                        while (i != 20){
			name[i] = msg_21.name[i];
                        i++;
                        }

                    }
                    break;
		case 24:
                    if( parse_ais_24( &ais, &msg_24 ) == 0 )
                    {
                        userid = msg_24.userid;
			get_timetag(&ais.six_state, &datetime);
			i = 0;
                        while (i != 20){
			name[i] = msg_24.name[i];
                        i++;
                        }

                    }
                    break;

            }  /* switch msgid */
            printf( "\"%09ld\",", userid );
            printf( "\"%d\",", cog );
	    printf( "\"%d\",", sog );
            printf( "\"%s\",", name );
            printf( "\"%d\",", year);
	    printf( "\"%i\",", month );
	    printf( "\"%i\",", day );
	    printf( "\"%i\",", hour );
	    printf( "\"%i\",", minute );
            printf( "\"%i\",", second );
            printf( "\"%0.6f\",", long_ddd );
            printf("\"%0.6f\"\r\n", lat_dd );
        }  /* if */
    }  /* while */

    
    return 0;
}
