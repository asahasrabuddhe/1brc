package onebrc

import (
	"bytes"
	"io"
	"os"
	"testing"
)

func TestProcess(t *testing.T) {
	type args struct {
		path     string
		expected string
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "10k",
			args: args{
				path:     "/Users/ajitem/1brc/measurements.txt",
				expected: "{Abha=23.7/23.7/23.7, Abidjan=31.0/31.0/31.0, Abéché=35.8/35.8/35.8, Accra=21.7/21.7/21.7, Addis Ababa=20.4/20.4/20.4, Adelaide=14.1/14.1/14.1, Aden=22.5/22.5/22.5, Ahvaz=7.4/7.4/7.4, Albuquerque=5.5/5.5/5.5, Alexandra=25.1/25.1/25.1, Alexandria=8.3/8.3/8.3, Algiers=8.0/8.0/8.0, Alice Springs=20.7/20.7/20.7, Almaty=13.1/13.1/13.1, Amsterdam=6.9/6.9/6.9, Anadyr=-3.2/-3.2/-3.2, Anchorage=12.8/12.8/12.8, Andorra la Vella=-1.1/-1.1/-1.1, Ankara=1.5/1.5/1.5, Antananarivo=11.0/11.0/11.0, Antsiranana=19.1/19.1/19.1, Arkhangelsk=11.3/11.3/11.3, Ashgabat=4.9/4.9/4.9, Asmara=26.4/26.4/26.4, Assab=41.2/41.2/41.2, Astana=21.6/21.6/21.6, Athens=0.7/0.7/0.7, Atlanta=16.2/16.2/16.2, Auckland=24.5/24.5/24.5, Austin=11.7/11.7/11.7, Baghdad=15.9/15.9/15.9, Baguio=12.1/12.1/12.1, Baku=21.9/21.9/21.9, Baltimore=20.7/20.7/20.7, Bamako=36.9/36.9/36.9, Bangkok=13.6/13.6/13.6, Bangui=39.7/39.7/39.7, Banjul=10.2/10.2/10.2, Barcelona=23.0/23.0/23.0, Bata=26.7/26.7/26.7, Batumi=9.5/9.5/9.5, Beijing=8.5/8.5/8.5, Beirut=20.5/20.5/20.5, Belgrade=17.3/17.3/17.3, Belize City=27.3/27.3/27.3, Benghazi=27.7/27.7/27.7, Bergen=21.6/21.6/21.6, Berlin=20.5/20.5/20.5, Bilbao=19.9/19.9/19.9, Birao=18.0/18.0/18.0, Bishkek=2.6/2.6/2.6, Bissau=26.2/26.2/26.2, Blantyre=32.8/32.8/32.8, Bloemfontein=-4.6/-4.6/-4.6, Boise=14.5/14.5/14.5, Bordeaux=21.1/21.1/21.1, Bosaso=28.9/28.9/28.9, Boston=4.6/4.6/4.6, Bouaké=23.7/23.7/23.7, Bratislava=7.0/7.0/7.0, Brazzaville=22.7/22.7/22.7, Bridgetown=12.9/12.9/12.9, Brisbane=22.4/22.4/22.4, Brussels=-5.6/-5.6/-5.6, Bucharest=-14.2/-14.2/-14.2, Budapest=18.7/18.7/18.7, Bujumbura=14.0/14.0/14.0, Bulawayo=36.0/36.0/36.0, Burnie=17.0/17.0/17.0, Busan=20.6/20.6/20.6, Cabo San Lucas=26.8/26.8/26.8, Cairns=4.9/4.9/4.9, Cairo=3.7/3.7/3.7, Calgary=10.2/10.2/10.2, Canberra=21.2/21.2/21.2, Cape Town=12.2/12.2/12.2, Changsha=7.1/7.1/7.1, Charlotte=19.2/19.2/19.2, Chiang Mai=9.3/9.3/9.3, Chicago=9.2/9.2/9.2, Chihuahua=17.6/17.6/17.6, Chittagong=24.8/24.8/24.8, Chișinău=18.3/18.3/18.3, Chongqing=10.5/10.5/10.5, Christchurch=9.5/9.5/9.5, City of San Marino=11.5/11.5/11.5, Colombo=26.5/26.5/26.5, Columbus=7.9/7.9/7.9, Conakry=34.3/34.3/34.3, Copenhagen=-3.9/-3.9/-3.9, Cotonou=15.3/15.3/15.3, Cracow=6.5/6.5/6.5, Da Lat=11.9/11.9/11.9, Da Nang=23.5/23.5/23.5, Dakar=28.5/28.5/28.5, Dallas=23.2/23.2/23.2, Damascus=9.7/9.7/9.7, Dampier=28.1/28.1/28.1, Dar es Salaam=27.5/27.5/27.5, Darwin=18.7/18.7/18.7, Denpasar=23.7/23.7/23.7, Denver=22.8/22.8/22.8, Detroit=18.4/18.4/18.4, Dhaka=24.3/24.3/24.3, Dikson=-4.1/-4.1/-4.1, Dili=39.0/39.0/39.0, Djibouti=28.4/28.4/28.4, Dodoma=19.4/19.4/19.4, Dolisie=23.4/23.4/23.4, Douala=26.3/26.3/26.3, Dubai=31.9/31.9/31.9, Dublin=4.9/4.9/4.9, Dunedin=11.5/11.5/11.5, Durban=18.8/18.8/18.8, Dushanbe=6.9/6.9/6.9, Edinburgh=-6.1/-6.1/-6.1, Edmonton=7.2/7.2/7.2, El Paso=34.1/34.1/34.1, Entebbe=8.5/8.5/8.5, Erbil=29.1/29.1/29.1, Erzurum=-12.1/-12.1/-12.1, Fairbanks=-1.1/-1.1/-1.1, Fianarantsoa=8.3/8.3/8.3, Flores,  Petén=11.9/11.9/11.9, Frankfurt=5.1/5.1/5.1, Fresno=36.0/36.0/36.0, Fukuoka=25.5/25.5/25.5, Gaborone=27.9/27.9/27.9, Gabès=17.7/17.7/17.7, Gagnoa=13.5/13.5/13.5, Gangtok=21.5/21.5/21.5, Garissa=22.2/22.2/22.2, Garoua=29.3/29.3/29.3, George Town=15.6/15.6/15.6, Ghanzi=6.6/6.6/6.6, Gjoa Haven=-11.7/-11.7/-11.7, Guadalajara=22.7/22.7/22.7, Guangzhou=33.8/33.8/33.8, Guatemala City=24.9/24.9/24.9, Halifax=9.6/9.6/9.6, Hamburg=-5.5/-5.5/-5.5, Hamilton=9.0/9.0/9.0, Hanga Roa=23.8/23.8/23.8, Hanoi=39.8/39.8/39.8, Harare=16.1/16.1/16.1, Harbin=10.2/10.2/10.2, Hargeisa=13.8/13.8/13.8, Hat Yai=45.8/45.8/45.8, Havana=31.6/31.6/31.6, Helsinki=21.4/21.4/21.4, Heraklion=17.5/17.5/17.5, Hiroshima=20.3/20.3/20.3, Ho Chi Minh City=23.0/23.0/23.0, Hobart=16.1/16.1/16.1, Hong Kong=22.6/22.6/22.6, Honiara=24.4/24.4/24.4, Honolulu=31.4/31.4/31.4, Houston=4.6/4.6/4.6, Ifrane=-2.2/-2.2/-2.2, Indianapolis=13.9/13.9/13.9, Iqaluit=-8.6/-8.6/-8.6, Irkutsk=9.4/9.4/9.4, Istanbul=2.0/2.0/2.0, Jacksonville=16.4/16.4/16.4, Jakarta=26.4/26.4/26.4, Jayapura=24.6/24.6/24.6, Jerusalem=23.2/23.2/23.2, Johannesburg=18.7/18.7/18.7, Jos=15.8/15.8/15.8, Juba=33.1/33.1/33.1, Kabul=8.5/8.5/8.5, Kampala=24.2/24.2/24.2, Kandi=11.8/11.8/11.8, Kankan=20.2/20.2/20.2, Kano=35.4/35.4/35.4, Kansas City=10.7/10.7/10.7, Karachi=6.1/6.1/6.1, Karonga=26.5/26.5/26.5, Kathmandu=20.8/20.8/20.8, Khartoum=29.5/29.5/29.5, Kingston=53.3/53.3/53.3, Kinshasa=16.2/16.2/16.2, Kolkata=19.5/19.5/19.5, Kuala Lumpur=18.8/18.8/18.8, Kumasi=24.4/24.4/24.4, Kunming=7.9/7.9/7.9, Kuopio=11.9/11.9/11.9, Kuwait City=25.6/25.6/25.6, Kyiv=15.3/15.3/15.3, Kyoto=22.2/22.2/22.2, La Ceiba=53.1/53.1/53.1, La Paz=20.2/20.2/20.2, Lagos=23.5/23.5/23.5, Lahore=48.0/48.0/48.0, Lake Havasu City=24.0/24.0/24.0, Lake Tekapo=-1.6/-1.6/-1.6, Las Palmas de Gran Canaria=6.6/6.6/6.6, Las Vegas=23.7/23.7/23.7, Launceston=0.7/0.7/0.7, Lhasa=-0.6/-0.6/-0.6, Libreville=30.9/30.9/30.9, Lisbon=21.4/21.4/21.4, Livingstone=20.2/20.2/20.2, Ljubljana=5.5/5.5/5.5, Lodwar=24.5/24.5/24.5, Lomé=24.6/24.6/24.6, London=2.8/2.8/2.8, Los Angeles=6.6/6.6/6.6, Louisville=4.9/4.9/4.9, Luanda=24.8/24.8/24.8, Lubumbashi=21.8/21.8/21.8, Lusaka=14.2/14.2/14.2, Luxembourg City=16.3/16.3/16.3, Lviv=8.4/8.4/8.4, Lyon=17.7/17.7/17.7, Madrid=20.6/20.6/20.6, Mahajanga=14.7/14.7/14.7, Makassar=11.1/11.1/11.1, Makurdi=19.0/19.0/19.0, Malabo=24.2/24.2/24.2, Malé=31.6/31.6/31.6, Managua=41.7/41.7/41.7, Manama=29.7/29.7/29.7, Mandalay=17.2/17.2/17.2, Mango=10.1/10.1/10.1, Manila=23.5/23.5/23.5, Maputo=33.5/33.5/33.5, Marrakesh=25.9/25.9/25.9, Marseille=29.5/29.5/29.5, Maun=49.6/49.6/49.6, Medan=17.8/17.8/17.8, Mek'ele=21.2/21.2/21.2, Melbourne=9.3/9.3/9.3, Memphis=27.5/27.5/27.5, Mexicali=18.1/18.1/18.1, Mexico City=13.1/13.1/13.1, Miami=38.4/38.4/38.4, Milan=12.2/12.2/12.2, Milwaukee=9.0/9.0/9.0, Minneapolis=-1.2/-1.2/-1.2, Minsk=8.1/8.1/8.1, Mogadishu=19.6/19.6/19.6, Mombasa=28.2/28.2/28.2, Monaco=21.5/21.5/21.5, Moncton=29.8/29.8/29.8, Monterrey=25.4/25.4/25.4, Montreal=14.4/14.4/14.4, Moscow=6.9/6.9/6.9, Mumbai=23.1/23.1/23.1, Murmansk=8.6/8.6/8.6, Muscat=25.2/25.2/25.2, Mzuzu=22.1/22.1/22.1, N'Djamena=14.5/14.5/14.5, Naha=12.9/12.9/12.9, Nairobi=19.0/19.0/19.0, Nakhon Ratchasima=28.6/28.6/28.6, Napier=17.7/17.7/17.7, Napoli=9.8/9.8/9.8, Nashville=-1.8/-1.8/-1.8, Nassau=24.6/24.6/24.6, Ndola=13.0/13.0/13.0, New Delhi=23.7/23.7/23.7, New Orleans=14.7/14.7/14.7, New York City=17.4/17.4/17.4, Ngaoundéré=12.4/12.4/12.4, Niamey=28.4/28.4/28.4, Nicosia=17.6/17.6/17.6, Niigata=23.1/23.1/23.1, Nouadhibou=22.8/22.8/22.8, Nouakchott=17.7/17.7/17.7, Novosibirsk=10.2/10.2/10.2, Nuuk=2.8/2.8/2.8, Odesa=6.8/6.8/6.8, Odienné=23.5/23.5/23.5, Oklahoma City=9.3/9.3/9.3, Omaha=10.9/10.9/10.9, Oranjestad=35.1/35.1/35.1, Oslo=12.0/12.0/12.0, Ottawa=11.3/11.3/11.3, Ouagadougou=20.6/20.6/20.6, Ouahigouya=36.2/36.2/36.2, Ouarzazate=6.0/6.0/6.0, Oulu=12.6/12.6/12.6, Palembang=47.2/47.2/47.2, Palermo=30.7/30.7/30.7, Palm Springs=33.0/33.0/33.0, Palmerston North=28.1/28.1/28.1, Panama City=13.6/13.6/13.6, Parakou=14.3/14.3/14.3, Paris=-7.8/-7.8/-7.8, Perth=24.9/24.9/24.9, Petropavlovsk-Kamchatsky=-1.9/-1.9/-1.9, Philadelphia=21.0/21.0/21.0, Phnom Penh=38.3/38.3/38.3, Phoenix=19.5/19.5/19.5, Pittsburgh=11.4/11.4/11.4, Podgorica=24.8/24.8/24.8, Pointe-Noire=23.7/23.7/23.7, Pontianak=21.9/21.9/21.9, Port Moresby=33.9/33.9/33.9, Port Sudan=45.5/45.5/45.5, Port Vila=17.5/17.5/17.5, Port-Gentil=30.5/30.5/30.5, Portland (OR)=10.2/10.2/10.2, Porto=6.2/6.2/6.2, Prague=-6.6/-6.6/-6.6, Praia=23.1/23.1/23.1, Pretoria=23.8/23.8/23.8, Pyongyang=13.9/13.9/13.9, Rabat=17.5/17.5/17.5, Rangpur=29.1/29.1/29.1, Reggane=21.1/21.1/21.1, Reykjavík=5.1/5.1/5.1, Riga=5.4/5.4/5.4, Riyadh=22.9/22.9/22.9, Rome=20.3/20.3/20.3, Roseau=23.6/23.6/23.6, Rostov-on-Don=19.7/19.7/19.7, Sacramento=12.4/12.4/12.4, Saint Petersburg=8.5/8.5/8.5, Saint-Pierre=17.1/17.1/17.1, Salt Lake City=20.6/20.6/20.6, San Antonio=20.4/20.4/20.4, San Diego=19.4/19.4/19.4, San Francisco=16.4/16.4/16.4, San Jose=18.1/18.1/18.1, San José=29.7/29.7/29.7, San Juan=39.7/39.7/39.7, San Salvador=23.4/23.4/23.4, Sana'a=22.2/22.2/22.2, Santo Domingo=23.5/23.5/23.5, Sapporo=0.5/0.5/0.5, Sarajevo=1.1/1.1/1.1, Saskatoon=3.4/3.4/3.4, Seattle=20.5/20.5/20.5, Seoul=2.0/2.0/2.0, Seville=24.1/24.1/24.1, Shanghai=10.6/10.6/10.6, Singapore=46.1/46.1/46.1, Skopje=4.0/4.0/4.0, Sochi=25.9/25.9/25.9, Sofia=30.2/30.2/30.2, Sokoto=27.7/27.7/27.7, Split=20.2/20.2/20.2, St. John's=11.2/11.2/11.2, St. Louis=12.3/12.3/12.3, Stockholm=-1.2/-1.2/-1.2, Surabaya=16.8/16.8/16.8, Suva=38.0/38.0/38.0, Suwałki=5.2/5.2/5.2, Sydney=12.8/12.8/12.8, Ségou=28.5/28.5/28.5, Tabora=24.0/24.0/24.0, Tabriz=7.1/7.1/7.1, Taipei=11.7/11.7/11.7, Tallinn=5.7/5.7/5.7, Tamale=30.6/30.6/30.6, Tamanrasset=17.9/17.9/17.9, Tampa=27.1/27.1/27.1, Tashkent=7.1/7.1/7.1, Tauranga=17.0/17.0/17.0, Tbilisi=10.7/10.7/10.7, Tegucigalpa=19.1/19.1/19.1, Tehran=22.0/22.0/22.0, Tel Aviv=22.8/22.8/22.8, Thessaloniki=13.2/13.2/13.2, Thiès=16.5/16.5/16.5, Tijuana=26.9/26.9/26.9, Timbuktu=30.3/30.3/30.3, Tirana=14.2/14.2/14.2, Toamasina=2.8/2.8/2.8, Tokyo=27.1/27.1/27.1, Toliara=8.1/8.1/8.1, Toluca=5.0/5.0/5.0, Toronto=17.2/17.2/17.2, Tripoli=15.4/15.4/15.4, Tromsø=-0.8/-0.8/-0.8, Tucson=27.5/27.5/27.5, Tunis=24.7/24.7/24.7, Ulaanbaatar=-1.2/-1.2/-1.2, Upington=33.0/33.0/33.0, Vaduz=12.5/12.5/12.5, Valencia=27.7/27.7/27.7, Valletta=3.0/3.0/3.0, Vancouver=11.8/11.8/11.8, Veracruz=19.5/19.5/19.5, Vienna=14.4/14.4/14.4, Vientiane=17.6/17.6/17.6, Villahermosa=24.4/24.4/24.4, Vilnius=10.9/10.9/10.9, Virginia Beach=24.8/24.8/24.8, Vladivostok=9.0/9.0/9.0, Warsaw=11.3/11.3/11.3, Washington, D.C.=20.8/20.8/20.8, Wau=15.5/15.5/15.5, Wellington=6.0/6.0/6.0, Whitehorse=15.2/15.2/15.2, Wichita=10.0/10.0/10.0, Willemstad=17.9/17.9/17.9, Winnipeg=13.8/13.8/13.8, Wrocław=1.0/1.0/1.0, Xi'an=9.1/9.1/9.1, Yakutsk=3.3/3.3/3.3, Yangon=27.6/27.6/27.6, Yaoundé=35.1/35.1/35.1, Yellowknife=-12.8/-12.8/-12.8, Yerevan=13.5/13.5/13.5, Yinchuan=-1.5/-1.5/-1.5, Zagreb=12.2/12.2/12.2, Zanzibar City=21.5/21.5/21.5, Zürich=13.9/13.9/13.9, Ürümqi=19.7/19.7/19.7, İzmir=20.1/20.1/20.1}\n",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			file, err := os.Open(tt.args.path)
			if err != nil {
				t.Error(err)
			}
			defer file.Close()

			var buf bytes.Buffer
			err = Process(file, &buf)
			if err != nil {
				t.Error(err)
			}

			if buf.String() != tt.args.expected {
				t.Errorf("expected: %s, got: %s", tt.args.expected, buf.String())
			}
		})
	}
}

func BenchmarkProcess(b *testing.B) {
	file, err := os.Open("/Users/ajitem/1brc/measurements.txt")
	if err != nil {
		b.Error(err)
	}
	defer file.Close()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err = Process(file, io.Discard)
		if err != nil {
			b.Error(err)
		}
	}
}
